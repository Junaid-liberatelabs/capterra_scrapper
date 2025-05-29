import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import sys

# --- Pydantic Models ---
from pydantic import BaseModel, Field, HttpUrl, ValidationError

class CapterraReviewTotals(BaseModel):
    review_count: Optional[int] = None
    overall_rating: Optional[str] = None
    ease_of_use_rating: Optional[str] = None
    customer_service_rating: Optional[str] = None
    functionality_rating: Optional[str] = None
    value_for_money_rating: Optional[str] = None

class CapterraIndividualReview(BaseModel):
    title: Optional[str] = None
    text: Optional[str] = ""
    reviewer: Optional[str] = None
    time_used_product: Optional[str] = None
    reviewer_avatar: Optional[HttpUrl] = None
    datetime: Optional[str] = Field(None, description="Formatted datetime string for output")
    rating: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None

class CapterraScrapeResultOutput(BaseModel):
    totals: Optional[CapterraReviewTotals] = None
    reviews: List[CapterraIndividualReview] = []
    product_name_scraped: Optional[str] = None
    product_category_scraped: Optional[str] = None
    original_url: HttpUrl
    reviews_count_scraped: int = 0
    scrape_duration_seconds: float

class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="Optional start date (YYYY-MM-DD) for reviews.")
    end_date_str: Optional[str] = Field(None, description="Optional end date (YYYY-MM-DD) for reviews.")

from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Capterra Scraper API - Focused Loader",
    description="Selenium loads all reviews, then BeautifulSoup parses. Uses DOM change wait.",
    version="1.2.6" # Incremented
)

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException,
    StaleElementReferenceException, ElementNotInteractableException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

try: from fake_useragent import UserAgent; ua = UserAgent()
except ImportError: print("Warning: fake-useragent not installed."); ua = None

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants for Capterra ---
SELENIUM_PAGE_TIMEOUT_S = 40
SELENIUM_ELEMENT_TIMEOUT_S = 15 
SELENIUM_INTERACTION_TIMEOUT_S = 10
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(3.0, 4.5)
# LOADING_SPINNER_SELECTOR = 'svg[class*="s1xr3lbz"]' # Kept for reference, but not primary wait
AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S = 20 # Timeout for waiting for new reviews after click

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10'

BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR = 'h1[data-testid="richcontent-title"]'
BS_PRODUCT_NAME_HEADER_SELECTOR = 'span.e1xzmg0z.h11hhycw.font-semibold'
BS_PRODUCT_CATEGORY_BREADCRUMB_SELECTOR = 'nav[class*="be9etqu"] a[data-testid="categoryslug"]'
BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR = 'div[class*="flex w-full flex-col justify-between gap-y-6"]'
BS_EASE_OF_USE_TOTAL_RATING_SELECTOR = f'{BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR} div:has(span:contains("Ease of use")) span.e1xzmg0z.sr2r3oj'
BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR = f'{BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR} div:has(span:contains("Customer Service")) span.e1xzmg0z.sr2r3oj'
BS_FEATURES_TOTAL_RATING_SELECTOR = f'{BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR} div:has(span:contains("Features")) span.e1xzmg0z.sr2r3oj'
BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR = f'{BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR} div:has(span:contains("Value for Money")) span.e1xzmg0z.sr2r3oj'
BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR = 'div[class*="sticky top-0"] div[class*="s1ncqr9d"] span[class*="sr2r3oj"]'
BS_REVIEW_COUNT_DISPLAY_SELECTOR = 'span.typo-30.font-semibold:contains("Reviews")'

BS_REVIEWER_NAME_SELECTOR = 'span.typo-20.text-neutral-99.font-semibold'
BS_REVIEWER_INFO_CONTAINER_SELECTOR = 'div.typo-10.text-neutral-90.w-full.lg\\:w-fit'
BS_REVIEWER_AVATAR_IMG_SELECTOR = 'img[data-testid="reviewer-profile-pic"]'
BS_REVIEWER_INITIALS_FALLBACK_SELECTOR = 'div.e1xzmg0z.ajdk2qt.bg-primary-20'
BS_REVIEW_TITLE_SELECTOR = 'h3.typo-20.font-semibold'
BS_REVIEW_DATE_PUBLISHED_SELECTOR = 'div.space-y-1 + div.typo-0.text-neutral-90'
BS_REVIEW_CARD_OVERALL_RATING_SELECTOR = 'div[data-testid="rating"] span.e1xzmg0z.sr2r3oj'
BS_REVIEW_PROS_SELECTOR = 'div.space-y-2:has(svg > title:contains("Positive icon")) > p'
BS_REVIEW_CONS_SELECTOR = 'div.space-y-2:has(svg > title:contains("Negative icon")) > p'
BS_REVIEW_TEXT_SELECTOR = 'div[class*="!mt-4 space-y-6"] > p:not(:has(svg))'


POPUP_CLOSE_SELECTORS_CAPTERRA = [
    "#onetrust-accept-btn-handler",
    'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"][class*="modal-close"]',
    "button[aria-label='Close' i]", 
    "button[class*='modal__close' i]", "button[class*='CloseButton']",
    "button[aria-label*='Dismiss' i]", "button[title*='Dismiss' i]",
    "div[role='dialog'] button[class*='close' i]",
    "div[id^='ZN_'] button[aria-label='Close']", 
    "button[id^='cookie-consent-accept']",
]

def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--disable-gpu")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
   
    options.add_argument('--log-level=3')
    options.add_argument("--start-maximized") 
    options.add_argument("--lang=en-US,en;q=0.9") 
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        return driver
    except Exception as e:
        print(f"  [Selenium Setup] CRITICAL ERROR: {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup Selenium driver: {e}")

def try_click(driver: webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S, thread_name: str = "DefaultThread"):
    try:
        WebDriverWait(driver, timeout).until(EC.visibility_of(element))
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", element)
        time.sleep(random.uniform(0.3, 0.6))
        element_id = (element.get_attribute('id') or "").lower()
        element_class = (element.get_attribute('class') or "").lower()
        if "popup" in thread_name.lower() or \
           "close" in thread_name.lower() or \
           "onetrust" in element_id or \
           "modal-close" in element_class:
            driver.execute_script("arguments[0].click();", element)
        else:
            element.click()
        return True
    except ElementClickInterceptedException:
        print(f"    [{thread_name}][try_click] Click intercepted. Trying JS click.")
        time.sleep(random.uniform(0.2, 0.4))
        try: driver.execute_script("arguments[0].click();", element); return True
        except Exception as e_js: print(f"    [{thread_name}][try_click] JS click failed: {e_js}"); return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException): return False
    except Exception as e_other_click:
        print(f"    [{thread_name}][try_click] Unexpected click error: {type(e_other_click).__name__} - {e_other_click}")
        return False

def attempt_to_close_popups_capterra(driver: webdriver.Chrome, thread_name: str):
    closed_any = False
    main_window = driver.current_window_handle
    # print(f"      [{thread_name}] Attempting to close Capterra popups...") 
    specific_popup_selector = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"]'
    try:
        specific_popups = driver.find_elements(By.CSS_SELECTOR, specific_popup_selector)
        for sp_btn in specific_popups:
            if sp_btn.is_displayed() and sp_btn.is_enabled():
                if try_click(driver, sp_btn, SELENIUM_INTERACTION_TIMEOUT_S / 2, f"{thread_name}-SpecificPopup"):
                    closed_any = True; time.sleep(0.7); 
                    break 
    except Exception: pass

    for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS_CAPTERRA):
        if sel == specific_popup_selector and closed_any: continue 
        current_closed_this_selector = False
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                if not iframe.is_displayed(): continue
                driver.switch_to.frame(iframe)
                popups_in_iframe = driver.find_elements(By.CSS_SELECTOR, sel)
                for p_btn in popups_in_iframe:
                    if p_btn.is_displayed() and p_btn.is_enabled():
                        if try_click(driver, p_btn, SELENIUM_INTERACTION_TIMEOUT_S / 3, f"{thread_name}-IframeP-{sel_idx}"):
                            closed_any = True; current_closed_this_selector = True; time.sleep(0.5)
                            break 
                driver.switch_to.default_content()
                if current_closed_this_selector: break 
            except Exception: driver.switch_to.default_content()
            if current_closed_this_selector: break
        if current_closed_this_selector: continue
        try: 
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            if not popups: continue
            for p_btn in popups:
                if p_btn.is_displayed() and p_btn.is_enabled():
                    if try_click(driver, p_btn, SELENIUM_INTERACTION_TIMEOUT_S / 2, f"{thread_name}-MainP-{sel_idx}"):
                        closed_any = True; current_closed_this_selector = True; time.sleep(0.5)
                        break
            if current_closed_this_selector and (not popups or (len(popups) > 0 and not popups[0].is_displayed())): break
        except Exception: pass
    
    final_handles = set(driver.window_handles)
    initial_handles_set = set([main_window]) 
    if len(final_handles) > len(initial_handles_set):
        new_handles = final_handles - initial_handles_set
        for handle in new_handles:
            try: driver.switch_to.window(handle); driver.close()
            except: pass
        driver.switch_to.window(main_window)
    if closed_any: print(f"      [{thread_name}] Popup handling actions taken."); time.sleep(random.uniform(0.5, 1.0))

def parse_capterra_datetime_for_output(date_str: str) -> Tuple[Optional[str], Optional[datetime]]:
    if not date_str: return None, None
    parsed_dt = None
    formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try:
            parsed_dt = datetime.strptime(date_str.strip(), fmt)
            break
        except ValueError: continue
    if parsed_dt:
        output_str = parsed_dt.strftime("%Y-%m-%d") + " 00:00:00 +0000" 
        return output_str, parsed_dt 
    return date_str, None

def _parse_individual_review_card_revised(review_card_soup: BeautifulSoup, thread_name: str) -> Optional[CapterraIndividualReview]:
    try:
        reviewer_name, reviewer_avatar_url, time_used = None, None, None

        reviewer_name_el = review_card_soup.select_one(BS_REVIEWER_NAME_SELECTOR)
        if reviewer_name_el: reviewer_name = reviewer_name_el.get_text(strip=True)

        avatar_el = review_card_soup.select_one(BS_REVIEWER_AVATAR_IMG_SELECTOR)
        if avatar_el and avatar_el.has_attr('src'): reviewer_avatar_url = avatar_el['src']
        
        if not reviewer_name: 
            initials_el = review_card_soup.select_one(BS_REVIEWER_INITIALS_FALLBACK_SELECTOR)
            if initials_el: reviewer_name = initials_el.get_text(strip=True)

        details_container = review_card_soup.select_one(BS_REVIEWER_INFO_CONTAINER_SELECTOR)
        if details_container:
            all_details_text = details_container.get_text(separator="\n", strip=True)
            time_used_match = re.search(r"Used the software for:\s*(.+)", all_details_text, re.IGNORECASE)
            if time_used_match: time_used = time_used_match.group(1).strip().rstrip('.')

        title_el = review_card_soup.select_one(BS_REVIEW_TITLE_SELECTOR)
        title = title_el.get_text(strip=True) if title_el else "No Title"
        
        date_str_on_page = "Unknown Date"
        date_el_found = None
        title_block_parent_scope = review_card_soup.select_one(f'{BS_REVIEW_TITLE_SELECTOR}')
        if title_block_parent_scope: title_block_parent_scope = title_block_parent_scope.parent

        if title_block_parent_scope:
            date_candidates = title_block_parent_scope.find_all('div', class_='typo-0 text-neutral-90', recursive=False)
            for cand in date_candidates:
                 if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', cand.get_text(strip=True), re.IGNORECASE):
                    date_el_found = cand; break
            if not date_el_found:
                current_el = title_block_parent_scope
                for _ in range(2): 
                    if not current_el: break
                    for sib in current_el.find_next_siblings('div', class_='typo-0 text-neutral-90', limit=2):
                        if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', sib.get_text(strip=True), re.IGNORECASE):
                            date_el_found = sib; break
                    if date_el_found: break
                    current_el = current_el.parent
        
        if date_el_found: date_str_on_page = date_el_found.get_text(strip=True)
        
        datetime_str_output, _ = parse_capterra_datetime_for_output(date_str_on_page)

        rating_el = review_card_soup.select_one(BS_REVIEW_CARD_OVERALL_RATING_SELECTOR)
        rating_str = rating_el.get_text(strip=True) if rating_el else "0.0"
        
        pros_el = review_card_soup.select_one(BS_REVIEW_PROS_SELECTOR)
        pros = pros_el.get_text(strip=True) if pros_el else None
        
        cons_el = review_card_soup.select_one(BS_REVIEW_CONS_SELECTOR)
        cons = cons_el.get_text(strip=True) if cons_el else None

        review_text = ""
        main_content_block = review_card_soup.select_one('div[class*="!mt-4 space-y-6"]')
        if main_content_block:
            p_tags = main_content_block.find_all('p', recursive=False)
            candidate_texts = []
            for p_tag in p_tags:
                p_text_content = p_tag.get_text(strip=True)
                is_pros_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Pros", re.I))))
                is_cons_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Cons", re.I))))
                if p_text_content and p_text_content != pros and p_text_content != cons and not is_pros_p and not is_cons_p:
                    candidate_texts.append(p_text_content)
            if candidate_texts: review_text = " ".join(candidate_texts)

        return CapterraIndividualReview(
            title=title, text=review_text, reviewer=reviewer_name,
            time_used_product=time_used, reviewer_avatar=reviewer_avatar_url,
            datetime=datetime_str_output, rating=rating_str,
            pros=pros, cons=cons
        )
    except Exception as e:
        return None

def _parse_capterra_html_for_reviews(
    page_source: str, original_url_str: str,
    selenium_product_name_guess: str,
    thread_name: str
) -> CapterraScrapeResultOutput:
    
    soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER)
    parsed_reviews_list: List[CapterraIndividualReview] = []
    
    product_name_scraped = selenium_product_name_guess
    product_category_scraped = None

    h1_title_el = soup.select_one(BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR)
    if h1_title_el and "reviews of" in h1_title_el.text.lower():
        product_name_scraped = h1_title_el.text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
    else:
        name_header_el = soup.select_one(BS_PRODUCT_NAME_HEADER_SELECTOR)
        if name_header_el: product_name_scraped = name_header_el.get_text(strip=True)

    cat_el = soup.select_one(BS_PRODUCT_CATEGORY_BREADCRUMB_SELECTOR)
    if cat_el: product_category_scraped = cat_el.get_text(strip=True)
    
    overall_rating_str, ease_of_use_str, customer_service_str, features_str, value_money_str = None, None, None, None, None
    review_count_from_display = None

    overall_product_rating_el = soup.select_one(BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR)
    if overall_product_rating_el:
        match = re.match(r"([\d\.]+)(?:\s*\((\d+)\))?", overall_product_rating_el.get_text(strip=True))
        if match:
            overall_rating_str = match.group(1)
            if match.group(2): review_count_from_display = int(match.group(2))

    if not review_count_from_display:
        review_count_display_el = soup.select_one(BS_REVIEW_COUNT_DISPLAY_SELECTOR)
        if review_count_display_el:
            text = review_count_display_el.get_text(strip=True)
            count_match_of = re.search(r"of\s+(\d+)\s+Reviews", text, re.IGNORECASE)
            count_match_showing_only = re.search(r"Showing\s+(\d+)\s+Reviews", text, re.IGNORECASE)
            if count_match_of: review_count_from_display = int(count_match_of.group(1))
            elif count_match_showing_only : review_count_from_display = int(count_match_showing_only.group(1))

    summary_section = soup.select_one(BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR)
    if summary_section:
        def get_rating_from_summary(selector):
            el = summary_section.select_one(selector)
            return el.get_text(strip=True).split()[0] if el and el.get_text(strip=True) else None
        ease_of_use_str = get_rating_from_summary(BS_EASE_OF_USE_TOTAL_RATING_SELECTOR)
        customer_service_str = get_rating_from_summary(BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR)
        features_str = get_rating_from_summary(BS_FEATURES_TOTAL_RATING_SELECTOR) 
        value_money_str = get_rating_from_summary(BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR)
    
    totals = CapterraReviewTotals(
        review_count=review_count_from_display, overall_rating=overall_rating_str,
        ease_of_use_rating=ease_of_use_str, customer_service_rating=customer_service_str,
        functionality_rating=features_str, value_for_money_rating=value_money_str
    )
    
    review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)
    if review_cards_container:
        review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR)
        for card_soup in review_card_soups:
            review = _parse_individual_review_card_revised(card_soup, thread_name)
            if review: parsed_reviews_list.append(review)
    else:
        print(f"  [{thread_name}] Review cards container not found in HTML for parsing by BS4.")

    return CapterraScrapeResultOutput(
        totals=totals, reviews=parsed_reviews_list,
        product_name_scraped=product_name_scraped, product_category_scraped=product_category_scraped,
        original_url=HttpUrl(original_url_str), reviews_count_scraped=len(parsed_reviews_list),
        scrape_duration_seconds=0 
    )

class WaitUntilReviewsLoadOrLoaderGone:
    def __init__(self, driver, initial_review_count, review_card_selector, loader_selector):
        self.driver = driver
        self.initial_review_count = initial_review_count
        self.review_card_selector = review_card_selector
        self.loader_selector = loader_selector
        self.loader_was_visible_after_click = False
        try:
            loader_elements = self.driver.find_elements(By.CSS_SELECTOR, self.loader_selector)
            if any(el.is_displayed() for el in loader_elements):
                self.loader_was_visible_after_click = True
        except: pass

    def __call__(self, driver):
        current_reviews = driver.find_elements(By.CSS_SELECTOR, self.review_card_selector)
        if len(current_reviews) > self.initial_review_count:
            return True
        if self.loader_was_visible_after_click:
            try:
                loader_elements = driver.find_elements(By.CSS_SELECTOR, self.loader_selector)
                if not any(el.is_displayed() for el in loader_elements):
                    time.sleep(0.1) 
                    current_reviews_after_loader = driver.find_elements(By.CSS_SELECTOR, self.review_card_selector)
                    return len(current_reviews_after_loader) > self.initial_review_count or True 
            except (StaleElementReferenceException, NoSuchElementException): return True 
        return False

def _load_all_capterra_reviews_selenium(
    product_url_str: str, company_slug: str
) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoad-{company_slug[:15]}"
    print(f"  [{thread_name}] Started Selenium loading phase for: {product_url_str}")
    driver = None
    product_name_guess = company_slug.replace("-"," ").title() 

    try:
        driver = setup_selenium_driver()
        driver.get(product_url_str)
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        attempt_to_close_popups_capterra(driver, thread_name)
        
        try:
            h1_el = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR)))
            if "reviews of" in h1_el.text.lower(): product_name_guess = h1_el.text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
        except: pass
        print(f"    [{thread_name}] Selenium phase - Product Name Context: {product_name_guess}")

        show_more_clicks = 0
        print(f"  [{thread_name}] Starting 'Show more reviews' loop...")
        
        try:
            WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR))
            )
        except TimeoutException:
            print(f"  [{thread_name}] Review container not found initially.")
            return driver.page_source, product_name_guess

        while True:
            initial_review_count_dom = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
            try:
                show_more_button = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2.5 ).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR))
                )
                if not show_more_button.is_enabled(): print(f"    [{thread_name}] 'Show more' not enabled."); break
                
                print(f"    [{thread_name}] Clicking 'Show More' (#{show_more_clicks + 1}). DOM reviews before: {initial_review_count_dom}.")
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", show_more_button)
                time.sleep(random.uniform(0.1, 0.3)) 
                
                clickable_button = WebDriverWait(driver, SELENIUM_INTERACTION_TIMEOUT_S / 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR)))
                
                driver.execute_script("arguments[0].click();", clickable_button)
                show_more_clicks += 1
                
                time.sleep(random.uniform(0.2, 0.5)) 
                
                print(f"      [{thread_name}] Click initiated. Waiting for content update (max {AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S}s)...")
                wait_condition = WaitUntilReviewsLoadOrLoaderGone(
                    driver, 
                    initial_review_count_dom, 
                    f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", 
                    LOADING_SPINNER_SELECTOR
                )
                try:
                    WebDriverWait(driver, AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S).until(wait_condition)
                    print(f"      [{thread_name}] Wait condition met.")
                except TimeoutException:
                    print(f"      [{thread_name}] Timeout waiting for custom condition. Fallback check.")
                    current_reviews_after_timeout = len(driver.find_elements(By.CSS_SELECTOR, f'{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}'))
                    if current_reviews_after_timeout == initial_review_count_dom:
                        print(f"      [{thread_name}] Review count unchanged after timeout. Assuming end of 'Show More' or issue.")
                        break 
                    else:
                        print(f"      [{thread_name}] Reviews DID increase ({initial_review_count_dom} -> {current_reviews_after_timeout}) despite EC timeout.")
                
                time.sleep(random.uniform(0.2, 0.5)) 
                attempt_to_close_popups_capterra(driver, thread_name)
                
                num_reviews_after_action = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
                if num_reviews_after_action == initial_review_count_dom and show_more_clicks > 0:
                    print(f"    [{thread_name}] DOM review count ({num_reviews_after_action}) did not increase after full wait cycle. Assuming end.")
                    break
                # print(f"    [{thread_name}] Loop iter done. DOM reviews now: {num_reviews_after_action}")

            except TimeoutException:
                print(f"  [{thread_name}] 'Show more reviews' button timed out in loop. Assuming all loaded.")
                break 
            except ElementClickInterceptedException:
                print(f"    [{thread_name}] 'Show more' click intercepted. Closing popups & retrying.")
                attempt_to_close_popups_capterra(driver, thread_name); time.sleep(1)
            except Exception as e_sm:
                print(f"  [{thread_name}] Error in 'Show more' loop: {type(e_sm).__name__} - {e_sm}")
                traceback.print_exc(file=sys.stdout); attempt_to_close_popups_capterra(driver, thread_name); time.sleep(0.5)
        
        print(f"  [{thread_name}] 'Show more' loop finished. Clicks: {show_more_clicks}. Retrieving page source.")
        final_page_source = driver.page_source
        return final_page_source, product_name_guess

    except Exception as e_load:
        print(f"  [{thread_name}] MAJOR ERROR during Selenium loading for {product_url_str}: {e_load}")
        traceback.print_exc()
        return None, product_name_guess
    finally:
        if driver: driver.quit(); print(f"  [{thread_name}] Selenium driver quit.")

def scrape_capterra_sync(
    product_url_str: str,
    start_date_filter_dt: Optional[datetime.date] = None, 
    end_date_filter_dt: Optional[datetime.date] = None
) -> Dict[str, Any]:
    overall_start_time = time.perf_counter()
    parsed_url = urlparse(product_url_str)
    path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
    company_slug = "unknown-slug"
    if len(path_segments) >= 3 and path_segments[0].lower() == "p": company_slug = path_segments[2] 

    print(f"Orchestrating Capterra scrape for: {product_url_str} (Slug: {company_slug})")
    
    page_source, product_name_selenium_guess = _load_all_capterra_reviews_selenium(product_url_str, company_slug)

    if not page_source:
        return {
            "status": "error", "message": "Failed to load page content with Selenium.",
            "data": None, "summary": {"product_url": product_url_str, "product_name_guess": product_name_selenium_guess}
        }

    thread_name_parse = f"CapterraParse-{company_slug[:15]}"
    print(f"  [{thread_name_parse}] Starting BS4 parsing phase...")
    
    parsed_data_obj = _parse_capterra_html_for_reviews(
        page_source, product_url_str, product_name_selenium_guess, thread_name_parse
    )
    
    parsed_data_obj.scrape_duration_seconds = round(time.perf_counter() - overall_start_time, 2)
    
    return {
        "status": "success" if parsed_data_obj.reviews_count_scraped > 0 else "no_reviews_found",
        "data": parsed_data_obj.model_dump(mode='json', by_alias=True),
        "summary": {
            "product_name": parsed_data_obj.product_name_scraped,
            "total_reviews_scraped": parsed_data_obj.reviews_count_scraped,
            "duration_seconds": parsed_data_obj.scrape_duration_seconds
        }
    }

@app.post("/scrape-capterra", tags=["Capterra"])
async def scrape_capterra_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    print(f"--- RAW REQUEST DATA RECEIVED (as Pydantic model) ---")
    try: print(request.model_dump_json(indent=2))
    except Exception as e: print(f"Could not dump request model: {e}")
    print(f"--- END RAW REQUEST DATA ---")

    start_date_filter_dt, end_date_filter_dt = None, None
    if hasattr(request, 'start_date_str') and request.start_date_str:
        try: start_date_filter_dt = datetime.strptime(request.start_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str format (YYYY-MM-DD).")
    if hasattr(request, 'end_date_str') and request.end_date_str:
        try: end_date_filter_dt = datetime.strptime(request.end_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str format (YYYY-MM-DD).")
    if start_date_filter_dt and end_date_filter_dt and start_date_filter_dt > end_date_filter_dt:
        raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
    if not request.urls: raise HTTPException(status_code=400, detail="No URLs provided.")

    print(f"API request for Capterra: {len(request.urls)} URLs (v{app.version}).")
    results: Dict[str, Dict[str, Any]] = {}
    max_workers = min(len(request.urls), (os.cpu_count() or 1) * 2, 4)
    print(f"  Using {max_workers} worker threads for Selenium tasks.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(scrape_capterra_sync, str(url_obj), start_date_filter_dt, end_date_filter_dt): str(url_obj)
            for url_obj in request.urls if ("capterra.com/p/" in str(url_obj) and "/reviews" in str(url_obj))
        }
        for url_obj in request.urls:
            if str(url_obj) not in future_to_url.values() and str(url_obj) not in results:
                 results[str(url_obj)] = {"status": "error", "message": f"Invalid Capterra URL format: {str(url_obj)} or skipped."}

        for future in as_completed(future_to_url):
            original_url_str = future_to_url[future]
            try: results[original_url_str] = future.result()
            except Exception as e:
                print(f"Task for {original_url_str} (Capterra v{app.version}) EXCEPTION in executor: {e}")
                traceback.print_exc(file=sys.stdout)
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(e).__name__}."}

    print(f"Finished Capterra API request processing (v{app.version}).")
    return results

# if __name__ == "__main__":
#     async def main_test_capterra():
#         # test_url_scholar = "https://www.capterra.com/p/135005/Scholar-LMS/reviews/" 
#         test_url_google = "https://www.capterra.com/p/253176/Google-One/reviews/" 
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url_google)])
#         results = await scrape_capterra_endpoint(test_request)
#         print(json.dumps(results, indent=2, default=str))
#     asyncio.run(main_test_capterra())
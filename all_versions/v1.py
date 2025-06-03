import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed # Ensure this is present
import traceback
import sys

# --- Pydantic Models ---
from pydantic import BaseModel, Field, HttpUrl, ValidationError

class ReviewerInfo(BaseModel):
    name: Optional[str] = None
    initials: Optional[str] = None
    profile_pic_url: Optional[HttpUrl] = None
    job_title: Optional[str] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None
    time_used_product: Optional[str] = None
    is_verified_linkedin: bool = False

class RatingDetail(BaseModel):
    category: str
    rating: float
    max_rating: Optional[int] = 5

class ProductLink(BaseModel):
    name: str
    logo_url: Optional[HttpUrl] = None
    url: Optional[HttpUrl] = None

class CapterraReview(BaseModel):
    review_id: Optional[str] = None
    reviewer: ReviewerInfo
    title: Optional[str] = None
    date_published_str: str
    date_published: Optional[datetime] = None
    overall_rating: float
    rating_details: List[RatingDetail] = []
    overall_comment: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None
    alternatives_considered: List[ProductLink] = []
    reason_for_choosing: Optional[str] = None
    switched_from: List[ProductLink] = []
    reason_for_switching: Optional[str] = None
    source_type: Optional[str] = None
    source_tooltip: Optional[str] = None

class ProductInfo(BaseModel):
    name: str
    capterra_url: HttpUrl
    logo_url: Optional[HttpUrl] = None
    overall_product_rating: Optional[float] = None
    total_product_reviews_count_header: Optional[int] = None
    category: Optional[str] = None

class CapterraScrapeResult(BaseModel):
    product_info: ProductInfo
    reviews: List[CapterraReview]
    reviews_count_scraped: int = 0
    scrape_duration_seconds: float

class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="Optional start date (YYYY-MM-DD) for reviews.")
    end_date_str: Optional[str] = Field(None, description="Optional end date (YYYY-MM-DD) for reviews.")

from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Capterra Scraper API - Undetected Loader",
    description="Selenium loads all reviews, then BeautifulSoup parses. Focus on stealth.",
    version="1.1.1" # Incremented
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
SELENIUM_PAGE_TIMEOUT_S = 45
SELENIUM_ELEMENT_TIMEOUT_S = 25
SELENIUM_INTERACTION_TIMEOUT_S = 15
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(3.5, 5.5)
AFTER_SHOW_MORE_CLICK_SLEEP_S = random.uniform(2.5, 4.0)

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
# MODIFIED SELECTOR HERE:
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10' # Removed leading '>'

PRODUCT_NAME_SELECTOR_HEADER = 'span.e1xzmg0z.h11hhycw.font-semibold' # !!! VERIFY AND UPDATE IF NEEDED !!!
PRODUCT_LOGO_SELECTOR_HEADER = 'figure.e1xzmg0z.tf6i4tz img'
PRODUCT_RATING_TEXT_SELECTOR_HEADER = 'div.flex.items-center.gap-1.hidden.lg\\:flex span.e1xzmg0z.sr2r3oj'
PRODUCT_CATEGORY_BREADCRUMB_SELECTOR = 'nav[class*="be9etqu"] a[data-testid="categoryslug"]'

REVIEWER_PROFILE_PIC_SELECTOR = 'img[data-testid="reviewer-profile-pic"]'
REVIEWER_INITIALS_SELECTOR = 'div.e1xzmg0z.ajdk2qt.bg-primary-20'
REVIEWER_NAME_SELECTOR = 'span.typo-20.text-neutral-99.font-semibold'
REVIEWER_DETAILS_BLOCK_SELECTOR = 'div.typo-10.text-neutral-90.w-full.lg\\:w-fit'
REVIEW_TITLE_SELECTOR = 'h3.typo-20.font-semibold'
REVIEW_DATE_SELECTOR = 'div.typo-0.text-neutral-90'
REVIEW_OVERALL_RATING_VALUE_SELECTOR = 'div[data-testid="rating"] span.e1xzmg0z.sr2r3oj'
REVIEW_RATINGS_DROPDOWN_CONTAINER_SELECTOR = 'div[role="dialog"].e1xzmg0z.c1ghu4k7.l1ix9ysh'
REVIEW_RATING_CATEGORY_ITEM_SELECTOR = REVIEW_RATINGS_DROPDOWN_CONTAINER_SELECTOR + ' div.typo-20.text-neutral-99.flex.cursor-pointer'
REVIEW_RATING_CATEGORY_NAME_SELECTOR = 'span.text-neutral-95.whitespace-nowrap'
REVIEW_RATING_CATEGORY_VALUE_SELECTOR = 'div[data-testid*="-rating"] span.e1xzmg0z.sr2r3oj'
REVIEW_OVERALL_COMMENT_SELECTOR = 'div.\\!mt-4.space-y-6 > p'
REVIEW_PROS_TEXT_SELECTOR = 'div.space-y-2:has(svg title:contains("Positive icon")) p'
REVIEW_CONS_TEXT_SELECTOR = 'div.space-y-2:has(svg title:contains("Negative icon")) p'
REVIEW_SOURCE_GROUP_SELECTOR = 'div[role="group"][aria-labelledby="review-source-label"]'
REVIEW_SOURCE_TOOLTIP_SELECTOR = REVIEW_SOURCE_GROUP_SELECTOR + ' + div[role="dialog"]'
REVIEW_ALTERNATIVES_SECTION_SELECTOR = 'div.space-y-4:has(span:contains("Alternatives considered"))'
REVIEW_SWITCHED_FROM_SECTION_SELECTOR = 'div.space-y-4:has(span:contains("Switched from"))'
REVIEW_PRODUCT_LINK_SELECTOR = 'a.e1xzmg0z.ljas29s.flex.items-center.gap-x-2'
REVIEW_PRODUCT_LINK_IMG_SELECTOR = 'figure img'
REVIEW_PRODUCT_LINK_NAME_SELECTOR = 'span.typo-10.whitespace-nowrap.font-normal'
REVIEW_REASON_SELECTOR = 'p'

POPUP_CLOSE_SELECTORS_CAPTERRA = [
    "#onetrust-accept-btn-handler",
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

    # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # options.add_experimental_option('useAutomationExtension', False)
    # options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("--window-size=1280,1024")
    # user_agent_str = ua.random if ua else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    # options.add_argument(f'user-agent={user_agent_str}')
    # options.add_argument('--log-level=3')
    # options.add_argument("--disable-infobars")
    # options.add_argument("--disable-popup-blocking")
    # options.add_argument("--start-maximized") 
    # options.add_argument("--lang=en-US,en;q=0.9") 
    # options.add_argument("--disable-features=UserAgentClientHint") 

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined
                });
            """
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
        time.sleep(random.uniform(0.5, 1.2))
        element.click()
        return True
    except ElementClickInterceptedException:
        print(f"    [{thread_name}][try_click] Click intercepted. Trying JS click after short delay.")
        time.sleep(random.uniform(0.3, 0.6))
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e_js:
            print(f"    [{thread_name}][try_click] JS click failed: {e_js}")
            return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException) as e_click:
        error_message = str(e_click).splitlines()[0] if str(e_click) else ""
        print(f"    [{thread_name}][try_click] Click failed: {type(e_click).__name__} - {error_message}")
        return False
    except Exception as e_other_click:
        print(f"    [{thread_name}][try_click] Unexpected click error: {type(e_other_click).__name__} - {e_other_click}")
        return False

def attempt_to_close_popups_capterra(driver: webdriver.Chrome, thread_name: str):
    closed_any = False
    main_window = driver.current_window_handle
    initial_handles = set(driver.window_handles)
    print(f"      [{thread_name}] Attempting to close Capterra popups...")

    for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS_CAPTERRA):
        current_closed_this_selector = False
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe_idx, iframe in enumerate(iframes):
            try:
                if not iframe.is_displayed(): continue
                driver.switch_to.frame(iframe)
                popups_in_iframe = driver.find_elements(By.CSS_SELECTOR, sel)
                for popup_btn_iframe in popups_in_iframe:
                    if popup_btn_iframe.is_displayed() and popup_btn_iframe.is_enabled():
                        if try_click(driver, popup_btn_iframe, SELENIUM_INTERACTION_TIMEOUT_S / 2, thread_name):
                            closed_any = True; current_closed_this_selector = True; time.sleep(0.7)
                            break 
                driver.switch_to.default_content()
                if current_closed_this_selector: break 
            except Exception: driver.switch_to.default_content()
            if current_closed_this_selector: break
        if current_closed_this_selector: continue

        try:
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            if not popups: continue
            for popup_btn in popups:
                if popup_btn.is_displayed() and popup_btn.is_enabled():
                    if try_click(driver, popup_btn, SELENIUM_INTERACTION_TIMEOUT_S / 2, thread_name):
                        closed_any = True; current_closed_this_selector = True; time.sleep(0.7)
                        break
            if current_closed_this_selector and (not popups or not popups[0].is_displayed()): break
        except Exception: pass
    
    final_handles = set(driver.window_handles)
    if len(final_handles) > len(initial_handles):
        new_handles = final_handles - initial_handles
        for handle in new_handles:
            if handle != main_window:
                try: driver.switch_to.window(handle); driver.close()
                except: pass
        driver.switch_to.window(main_window)

    if closed_any: print(f"      [{thread_name}] Popup handling finished. Pausing briefly."); time.sleep(random.uniform(0.8, 1.2))

def parse_date_capterra(date_str: str) -> Optional[datetime]:
    if not date_str: return None
    formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try: return datetime.strptime(date_str.strip(), fmt)
        except ValueError: continue
    # print(f"      [Date Parse] Warning: Could not parse date: {date_str}") # Reduce noise
    return None

def _parse_individual_review_card(review_card_soup: BeautifulSoup, base_url: str, thread_name: str) -> Optional[CapterraReview]:
    try:
        reviewer_name_el = review_card_soup.select_one(REVIEWER_NAME_SELECTOR)
        reviewer_name = reviewer_name_el.get_text(strip=True) if reviewer_name_el else None

        reviewer_details_block = review_card_soup.select_one(REVIEWER_DETAILS_BLOCK_SELECTOR)
        job_title, industry, company_size_text, time_used = None, None, None, None
        if reviewer_details_block:
            details_texts = [s.strip() for s in reviewer_details_block.stripped_strings]
            current_details = list(details_texts) 
            if current_details and reviewer_name and current_details[0] == reviewer_name: current_details.pop(0)
            if current_details: job_title = current_details.pop(0)
            
            industry_parts = []
            while current_details and "used the software for:" not in current_details[0].lower() and not re.search(r'\d+\s*-\s*\d+\s*employees|\d+\+\s*employees|Self-employed', current_details[0], re.IGNORECASE):
                industry_parts.append(current_details.pop(0))
            industry = ", ".join(industry_parts) if industry_parts else None

            if current_details and re.search(r'\d+\s*-\s*\d+\s*employees|\d+\+\s*employees|Self-employed', current_details[0], re.IGNORECASE):
                company_size_text = current_details.pop(0)
            
            if current_details and "used the software for:" in current_details[0].lower():
                time_used = current_details.pop(0).replace("Used the software for:", "").strip()

        profile_pic_el = review_card_soup.select_one(REVIEWER_PROFILE_PIC_SELECTOR)
        profile_pic_url = profile_pic_el['src'] if profile_pic_el and profile_pic_el.has_attr('src') else None
        initials = None
        if not profile_pic_url:
            initials_el = review_card_soup.select_one(REVIEWER_INITIALS_SELECTOR)
            if initials_el: initials = initials_el.get_text(strip=True)

        is_verified_linkedin = bool(review_card_soup.select_one('i[class*="icon-linkedin"]'))
        if not reviewer_name and initials and "VR" in initials :
            reviewer_name = "Verified Reviewer"

        reviewer_info = ReviewerInfo(
            name=reviewer_name, initials=initials, profile_pic_url=profile_pic_url,
            job_title=job_title, industry=industry, company_size=company_size_text,
            time_used_product=time_used, is_verified_linkedin=is_verified_linkedin
        )

        title_el = review_card_soup.select_one(REVIEW_TITLE_SELECTOR)
        title = title_el.get_text(strip=True) if title_el else None
        
        date_el = None
        possible_date_els = review_card_soup.select(REVIEW_DATE_SELECTOR)
        for p_date_el in possible_date_els: 
            if title_el and p_date_el in title_el.parent.find_all(recursive=False, limit=5):
                date_el = p_date_el; break
        if not date_el and possible_date_els: date_el = possible_date_els[0] 

        date_published_str = date_el.get_text(strip=True) if date_el else "Unknown Date"
        date_published_dt = parse_date_capterra(date_published_str)

        overall_rating_el = review_card_soup.select_one(REVIEW_OVERALL_RATING_VALUE_SELECTOR)
        overall_rating = float(overall_rating_el.get_text(strip=True)) if overall_rating_el else 0.0

        rating_details_list = []
        ratings_dropdown_container = review_card_soup.select_one(REVIEW_RATINGS_DROPDOWN_CONTAINER_SELECTOR)
        if ratings_dropdown_container:
            rating_items = ratings_dropdown_container.select(REVIEW_RATING_CATEGORY_ITEM_SELECTOR)
            for item in rating_items:
                cat_name_el = item.select_one(REVIEW_RATING_CATEGORY_NAME_SELECTOR)
                val_el_text_content = item.get_text()
                if cat_name_el and val_el_text_content:
                    cat_name = cat_name_el.get_text(strip=True)
                    max_r, parsed_val = 5, None
                    if "Likelihood to Recommend" in cat_name:
                        max_r = 10
                        match_recommend = re.search(r"(\d+)\s*/\s*10", val_el_text_content)
                        if match_recommend: parsed_val = float(match_recommend.group(1))
                    else: 
                        star_val_el = item.select_one(REVIEW_RATING_CATEGORY_VALUE_SELECTOR)
                        if star_val_el:
                            try: parsed_val = float(star_val_el.get_text(strip=True).split()[0])
                            except: pass
                    if parsed_val is not None: rating_details_list.append(RatingDetail(category=cat_name, rating=parsed_val, max_rating=max_r))
                    # else: print(f"      [{thread_name}] Warning: Could not parse rating value for '{cat_name}' from: '{val_el_text_content}'")
        
        pros_el = review_card_soup.select_one(REVIEW_PROS_TEXT_SELECTOR)
        pros_text = pros_el.get_text(strip=True) if pros_el else None
        cons_el = review_card_soup.select_one(REVIEW_CONS_TEXT_SELECTOR)
        cons_text = cons_el.get_text(strip=True) if cons_el else None
        
        overall_comment_text = None
        overall_comment_candidates = review_card_soup.select('div[class*="!mt-4 space-y-6"] > p')
        if overall_comment_candidates:
            temp_overall_text = overall_comment_candidates[0].get_text(strip=True)
            is_pros_or_cons = (temp_overall_text == pros_text) or (temp_overall_text == cons_text)
            if not is_pros_or_cons: overall_comment_text = temp_overall_text
        
        source_type, source_tooltip = None, None
        source_group = review_card_soup.select_one(REVIEW_SOURCE_GROUP_SELECTOR)
        if source_group:
            source_text_el = source_group.find('span', string=lambda t: t and t.strip() and t.strip() != "Review Source")
            if source_text_el: source_type = source_text_el.get_text(strip=True)
            
            tooltip_el = review_card_soup.select_one(f'div[role="dialog"][class*="l1ix9ysh"]')
            if tooltip_el and source_group.find_next_sibling('div', role='dialog') == tooltip_el :
                source_tooltip = tooltip_el.get_text(strip=True)

        alternatives_considered, reason_for_choosing = [], None
        switched_from, reason_for_switching = [], None

        for section_type_selector, target_list, reason_var_name_str in [
            (REVIEW_SWITCHED_FROM_SECTION_SELECTOR, switched_from, "reason_for_switching"),
            (REVIEW_ALTERNATIVES_SECTION_SELECTOR, alternatives_considered, "reason_for_choosing")]:
            section_el = review_card_soup.select_one(section_type_selector)
            if section_el:
                product_links_els = section_el.select(REVIEW_PRODUCT_LINK_SELECTOR)
                for link_el in product_links_els:
                    name_el = link_el.select_one(REVIEW_PRODUCT_LINK_NAME_SELECTOR)
                    name = name_el.get_text(strip=True) if name_el else "Unknown Product"
                    logo_el = link_el.select_one(REVIEW_PRODUCT_LINK_IMG_SELECTOR)
                    logo_url = logo_el['src'] if logo_el and logo_el.has_attr('src') else None
                    url = urljoin(base_url, link_el['href']) if link_el.has_attr('href') else None
                    target_list.append(ProductLink(name=name, logo_url=logo_url, url=url))
                
                reason_p_el = section_el.find('p', recursive=False) 
                if not reason_p_el: 
                    div_child = section_el.find('div', recursive=False)
                    if div_child: reason_p_el = div_child.find('p',recursive=False)
                if reason_p_el:
                    reason_text = reason_p_el.get_text(strip=True)
                    if reason_var_name_str == "reason_for_switching": reason_for_switching = reason_text
                    elif reason_var_name_str == "reason_for_choosing": reason_for_choosing = reason_text
        
        return CapterraReview(
            reviewer=reviewer_info, title=title, date_published_str=date_published_str, date_published=date_published_dt,
            overall_rating=overall_rating, rating_details=rating_details_list,
            overall_comment=overall_comment_text, pros=pros_text, cons=cons_text,
            source_type=source_type, source_tooltip=source_tooltip,
            alternatives_considered=alternatives_considered, reason_for_choosing=reason_for_choosing,
            switched_from=switched_from, reason_for_switching=reason_for_switching
        )
    except Exception as e:
        print(f"      [{thread_name}] Error parsing individual review card: {type(e).__name__} - {e}")
        # traceback.print_exc(file=sys.stdout)
        return None

def _scrape_capterra_page_for_reviews(
    product_url_str: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Optional[CapterraScrapeResult]:
    thread_name = f"Capterra-{company_slug[:15]}"
    scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for: {product_url_str}")
    
    driver = None
    try:
        driver = setup_selenium_driver()
        print(f"  [{thread_name}] Navigating to {product_url_str}...")
        driver.get(product_url_str)
        print(f"  [{thread_name}] Page loaded. Sleeping for {INITIAL_PAGE_LOAD_SLEEP_S:.2f}s for dynamic content...")
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        attempt_to_close_popups_capterra(driver, thread_name)

        print(f"  [{thread_name}] Attempting to extract initial product information...")
        product_name, product_logo_url, category_name = "Unknown Product", None, None
        overall_rating_val, total_reviews_count_hdr = None, None

        try:
            WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="sticky top-0"]'))
            )
            print(f"    [{thread_name}] Main header container found. Proceeding to extract product details.")
            try:
                product_name_el = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR_HEADER))
                )
                product_name = product_name_el.text.strip()
                print(f"    [{thread_name}] Product Name from specific selector: {product_name}")
            except TimeoutException:
                print(f"    [{thread_name}] Specific product name selector ('{PRODUCT_NAME_SELECTOR_HEADER}') timed out.")
                try: 
                    h1_elements = driver.find_elements(By.TAG_NAME, "h1")
                    for h1_el in h1_elements:
                        if "reviews of" in h1_el.text.lower():
                            product_name_candidate = h1_el.text.lower().replace("reviews of", "").strip().title()
                            # Further check if this is likely the main product name
                            if company_slug.lower().replace("-"," ") in product_name_candidate.lower():
                                product_name = product_name_candidate
                                print(f"    [{thread_name}] Fallback product name from H1: {product_name}")
                                break
                    if product_name == "Unknown Product": # If still not found
                         print(f"    [{thread_name}] Could not determine product name from H1 tags either.")
                except Exception as e_h1_fallback:
                     print(f"    [{thread_name}] Error in H1 fallback for product name: {e_h1_fallback}")

            try: product_logo_url = driver.find_element(By.CSS_SELECTOR, PRODUCT_LOGO_SELECTOR_HEADER).get_attribute('src')
            except NoSuchElementException: print(f"    [{thread_name}] Product logo not found with selector: {PRODUCT_LOGO_SELECTOR_HEADER}")
            
            try:
                rating_text_el = driver.find_element(By.CSS_SELECTOR, PRODUCT_RATING_TEXT_SELECTOR_HEADER)
                rating_text = rating_text_el.text.strip()
                match = re.match(r"([\d\.]+)\s*\((\d+)\)", rating_text)
                if match: overall_rating_val, total_reviews_count_hdr = float(match.group(1)), int(match.group(2))
            except NoSuchElementException: print(f"    [{thread_name}] Product rating text not found with selector: {PRODUCT_RATING_TEXT_SELECTOR_HEADER}")
            except Exception as e_hdr_rating: print(f"    [{thread_name}] Minor error parsing header rating: {e_hdr_rating}")

            try: category_name = driver.find_element(By.CSS_SELECTOR, PRODUCT_CATEGORY_BREADCRUMB_SELECTOR).text.strip()
            except NoSuchElementException: print(f"    [{thread_name}] Product category breadcrumb not found: {PRODUCT_CATEGORY_BREADCRUMB_SELECTOR}")
        except TimeoutException:
            print(f"  [{thread_name}] WARNING: Header section did not load key elements within timeout. Product info might be incomplete.")
        except Exception as e_prod_info:
            print(f"  [{thread_name}] Error during product info extraction: {e_prod_info}")

        product_info = ProductInfo(
            name=product_name if product_name != "Unknown Product" else company_slug.replace("-"," ").title(), # Better default
            capterra_url=HttpUrl(product_url_str), logo_url=product_logo_url,
            overall_product_rating=overall_rating_val, total_product_reviews_count_header=total_reviews_count_hdr,
            category=category_name
        )
        print(f"  [{thread_name}] Product Info Extracted: Name='{product_info.name}', Category='{product_info.category}', HeaderReviews='{product_info.total_product_reviews_count_header or 'N/A'}'")

        show_more_clicks = 0
        max_show_more_clicks = 200 
        if total_reviews_count_hdr and total_reviews_count_hdr > 20: # Slightly reduced base number of reviews per load
            max_show_more_clicks = (total_reviews_count_hdr // 20) + 10 

        print(f"  [{thread_name}] Starting 'Show more reviews' loop (max clicks: {max_show_more_clicks})...")
        consecutive_no_new_reviews = 0

        while show_more_clicks < max_show_more_clicks:
            reviews_elements_before_click = driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}")
            num_reviews_before_click = len(reviews_elements_before_click)
            
            try:
                WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2 ).until( # Wait for container before button
                    EC.presence_of_element_located((By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR))
                )
                show_more_button = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2.5 ).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR))
                ) 
                
                if not show_more_button.is_enabled():
                    print(f"  [{thread_name}] 'Show more' button found but not enabled. Assuming end of reviews.")
                    break
                
                print(f"  [{thread_name}] Clicking 'Show more reviews' (Click #{show_more_clicks + 1}). DOM reviews: {num_reviews_before_click}")
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", show_more_button)
                time.sleep(random.uniform(0.4, 0.8)) 
                WebDriverWait(driver, SELENIUM_INTERACTION_TIMEOUT_S).until(EC.element_to_be_clickable(show_more_button))
                
                # Try JS click first as it can be more robust with overlays
                try:
                    driver.execute_script("arguments[0].click();", show_more_button)
                except Exception: # Fallback to Selenium click
                    print(f"    [{thread_name}] JS click failed for 'Show More', trying Selenium click.")
                    show_more_button.click()
                
                show_more_clicks += 1
                print(f"    [{thread_name}] Clicked. Sleeping for {AFTER_SHOW_MORE_CLICK_SLEEP_S:.2f}s...")
                time.sleep(AFTER_SHOW_MORE_CLICK_SLEEP_S)
                
                reviews_elements_after_click = driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}")
                num_reviews_after_click = len(reviews_elements_after_click)

                if num_reviews_after_click == num_reviews_before_click:
                    consecutive_no_new_reviews += 1
                    print(f"    [{thread_name}] No new reviews loaded this click. Consecutive fails: {consecutive_no_new_reviews}")
                    if consecutive_no_new_reviews >= 3: 
                        print(f"  [{thread_name}] No new reviews loaded for {consecutive_no_new_reviews} consecutive clicks. Assuming end.")
                        break
                else:
                    consecutive_no_new_reviews = 0 

                if total_reviews_count_hdr and num_reviews_after_click >= total_reviews_count_hdr:
                    print(f"  [{thread_name}] DOM review count ({num_reviews_after_click}) reached/exceeded header count ({total_reviews_count_hdr}).")
                    break

            except TimeoutException:
                print(f"  [{thread_name}] 'Show more reviews' button not found or not visible/clickable after waiting. Assuming all reviews are loaded.")
                break
            except Exception as e_sm:
                print(f"  [{thread_name}] Error during 'Show more' click loop: {type(e_sm).__name__} - {e_sm}")
                traceback.print_exc(file=sys.stdout)
                attempt_to_close_popups_capterra(driver, thread_name)
                time.sleep(1) 
        
        print(f"  [{thread_name}] Finished 'Show more' loop. Total clicks: {show_more_clicks}.")
        print(f"  [{thread_name}] Retrieving final page source for parsing...")
        page_source = driver.page_source
        
        print(f"  [{thread_name}] Quitting Selenium driver BEFORE parsing...")
        driver.quit()
        driver = None 
        print(f"  [{thread_name}] Selenium driver quit. Starting BeautifulSoup parsing...")

        soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER)
        all_reviews_on_page: List[CapterraReview] = []
        review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)

        if review_cards_container:
            review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR)
            print(f"  [{thread_name}] Found {len(review_card_soups)} review card elements in HTML for parsing.")
            
            for idx, card_soup in enumerate(review_card_soups):
                review = _parse_individual_review_card(card_soup, product_url_str, thread_name)
                if review:
                    if start_date_filter and review.date_published and review.date_published < start_date_filter: continue
                    if end_date_filter and review.date_published and review.date_published > end_date_filter: continue
                    all_reviews_on_page.append(review)
        else:
            print(f"  [{thread_name}] CRITICAL: Review cards container ('{REVIEW_CARDS_CONTAINER_SELECTOR}') not found in final page source.")

        scrape_duration = time.perf_counter() - scrape_start_time
        print(f"  [{thread_name}] Finished scraping & parsing {product_url_str} in {scrape_duration:.2f}s. Parsed {len(all_reviews_on_page)} reviews.")
        
        return CapterraScrapeResult(
            product_info=product_info,
            reviews=all_reviews_on_page,
            reviews_count_scraped=len(all_reviews_on_page),
            scrape_duration_seconds=round(scrape_duration, 2)
        )

    except Exception as e_main_scrape:
        print(f"  [{thread_name}] MAJOR ERROR during Capterra scrape for {product_url_str}: {type(e_main_scrape).__name__} - {e_main_scrape}")
        traceback.print_exc()
        if driver: 
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                # driver.save_screenshot(f"ERROR_{thread_name}_{timestamp}.png") # Uncomment for debugging
                # with open(f"ERROR_{thread_name}_{timestamp}.html", "w", encoding="utf-8") as f: # Uncomment for debugging
                #     f.write(driver.page_source)
                # print(f"    [{thread_name}] Saved error screenshot and page source (if enabled).")
            except Exception as e_save:
                print(f"    [{thread_name}] Could not save error screenshot/source: {e_save}")
        return None
    finally:
        if driver: 
            driver.quit()
            print(f"  [{thread_name}] WebDriver quit in finally block.")


def scrape_capterra_sync(
    product_url_str: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    parsed_url = urlparse(product_url_str)
    path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
    company_slug = "unknown-product"
    if len(path_segments) >= 3 and path_segments[0].lower() == "p":
        company_slug = path_segments[2] 

    print(f"Orchestrating Capterra scrape for: {product_url_str} (Slug: {company_slug})")
    result_obj = _scrape_capterra_page_for_reviews(product_url_str, company_slug, start_date_filter, end_date_filter)

    if result_obj:
        return {
            "status": "success" if result_obj.reviews_count_scraped > 0 else "no_reviews_found",
            "data": result_obj.model_dump(mode='json'),
            "summary": {
                "product_name": result_obj.product_info.name,
                "total_reviews_scraped": result_obj.reviews_count_scraped,
                "duration_seconds": result_obj.scrape_duration_seconds
            }
        }
    else:
        return {
            "status": "error",
            "message": f"Scraping failed for {product_url_str}. Check server logs.",
            "data": None,
            "summary": {"product_url": product_url_str, "product_name_guess": company_slug}
        }

@app.post("/scrape-capterra", tags=["Capterra"])
async def scrape_capterra_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    start_date_filter, end_date_filter = None, None
    if request.start_date_str:
        try: start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str format (YYYY-MM-DD).")
    if request.end_date_str:
        try: end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str format (YYYY-MM-DD).")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter:
        raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
    if not request.urls: raise HTTPException(status_code=400, detail="No URLs provided.")

    print(f"API request for Capterra: {len(request.urls)} URLs (v{app.version}).")
    results: Dict[str, Dict[str, Any]] = {}
    
    with ThreadPoolExecutor(max_workers=min(len(request.urls), 4)) as executor:
        future_to_url = {}
        for url_obj in request.urls:
            url_str = str(url_obj)
            if "capterra.com/p/" not in url_str or "/reviews" not in url_str:
                 results[url_str] = {"status": "error", "message": f"Invalid Capterra URL format: {url_str}."}
                 continue
            future = executor.submit(scrape_capterra_sync, url_str, start_date_filter, end_date_filter)
            future_to_url[future] = url_str

        for future in as_completed(future_to_url): # as_completed is correctly imported and used
            original_url_str = future_to_url[future]
            try:
                scrape_result = future.result()
                results[original_url_str] = scrape_result
            except Exception as e:
                print(f"Task for {original_url_str} (Capterra v{app.version}) EXCEPTION in executor: {e}")
                traceback.print_exc(file=sys.stdout)
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed in executor: {type(e).__name__}."}

    print(f"Finished Capterra API request processing (v{app.version}).")
    return results

# For local testing
# if __name__ == "__main__":
#     async def main_test_capterra():
#         # Test with a URL that previously caused issues to ensure fixes work
#         test_url_scholar = "https://www.capterra.com/p/135005/Scholar-LMS/reviews/"
#         test_url_google = "https://www.capterra.com/p/253176/Google-One/reviews/"
        
#         # Test one URL at a time first for easier debugging
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url_scholar)])
#         # test_request = ScrapeRequest(urls=[HttpUrl(test_url_google)])
#         # test_request = ScrapeRequest(urls=[HttpUrl(test_url_scholar), HttpUrl(test_url_google)]) # Test multiple
        
#         results = await scrape_capterra_endpoint(test_request)
#         print(json.dumps(results, indent=2, default=str))
#     asyncio.run(main_test_capterra())
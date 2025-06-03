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

# --- Pydantic Models (Revised for new JSON structure) ---
from pydantic import BaseModel, Field, HttpUrl, ValidationError, validator

class CapterraReviewTotals(BaseModel):
    review_count: Optional[int] = None
    overall_rating: Optional[str] = None # Keep as string as per example "4.7"
    ease_of_use_rating: Optional[str] = None
    customer_service_rating: Optional[str] = None
    functionality_rating: Optional[str] = None # Assuming this is 'Features'
    value_for_money_rating: Optional[str] = None

class CapterraIndividualReview(BaseModel):
    title: Optional[str] = None
    text: Optional[str] = "" # Main review body, defaults to empty string
    reviewer: Optional[str] = None
    reviewer_avatar: Optional[HttpUrl] = None
    id: Optional[str] = None # Extracted if available
    datetime_str: Optional[str] = Field(None, alias="datetime") # Raw string from site
    datetime_obj: Optional[datetime] = None # Parsed datetime
    rating: Optional[str] = None # Overall rating for this specific review
    url: Optional[HttpUrl] = None # URL to the specific review (might not be easily available)
    pros: Optional[str] = None
    cons: Optional[str] = None

    @validator('datetime_obj', pre=True, always=True)
    def parse_datetime_str(cls, v, values):
        datetime_str = values.get('datetime_str')
        if datetime_str:
            # Example format: "2022-06-01 06:18:16 -0400"
            # Capterra format seems to be "Month Day, Year" e.g. "June 1, 2022"
            # We will parse the date part and ignore timezone for simplicity here.
            # The date comes from REVIEW_DATE_SELECTOR in the previous parsing.
            # This model is based on the *desired output JSON*, not directly what's on page.
            # The actual parsing logic in _parse_individual_review_card_revised will handle Capterra's format.
            # For now, this validator is a placeholder if we were to parse the output JSON's datetime_str.
            # For now, datetime_obj will be populated by the parsing function directly.
            return v
        return None


class CapterraScrapeResultOutput(BaseModel): # Renamed to avoid conflict
    totals: Optional[CapterraReviewTotals] = None
    reviews: List[CapterraIndividualReview] = []
    product_name_scraped: Optional[str] = None
    product_category_scraped: Optional[str] = None
    original_url: HttpUrl
    reviews_count_scraped: int = 0
    scrape_duration_seconds: float


class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="Optional start date (YYYY-MM-DD) for reviews.") # Less relevant with new JSON
    end_date_str: Optional[str] = Field(None, description="Optional end date (YYYY-MM-DD) for reviews.") # Less relevant

from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Capterra Scraper API - Undetected Loader",
    description="Selenium loads all reviews, then BeautifulSoup parses. Focus on stealth.",
    version="1.1.2" # Incremented
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
SELENIUM_ELEMENT_TIMEOUT_S = 20 # Reduced from 25, can be tuned
SELENIUM_INTERACTION_TIMEOUT_S = 10 # Reduced from 15
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(4.0, 6.0) # Increased
AFTER_SHOW_MORE_CLICK_SLEEP_S = random.uniform(3.0, 5.0) # Increased

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10'

# --- Product Header Info Selectors (Still needed for context) ---
# !!! IMPORTANT: Verify these selectors for each target page structure !!!
PRODUCT_NAME_HEADER_FALLBACK_H1_SELECTOR = 'h1[data-testid="richcontent-title"]' # e.g., "Reviews of Scholar LMS"
PRODUCT_LOGO_SELECTOR_HEADER = 'div[class*="sticky top-0"] figure[class*="tf6i4tz"] img' # More specific parent for logo
PRODUCT_RATING_SUMMARY_SECTION_SELECTOR = 'div[class*="flex w-full flex-col justify-between gap-y-6"]' # Parent of overall rating stats

# --- Selectors for Totals (within PRODUCT_RATING_SUMMARY_SECTION_SELECTOR or similar) ---
# These need careful inspection of where the overall "Ease of use 4.7", "Customer Service 4.2" etc. are located.
# The example totals JSON does not directly map to easily selectable individual elements from capterra.html snippet.
# For now, these will be parsed from the detailed review breakdown if available.
# Or, if there's a main summary block:
EASE_OF_USE_TOTAL_RATING_SELECTOR = 'div.flex.w-full.items-center.justify-between:has(span:contains("Ease of use")) span.e1xzmg0z.sr2r3oj'
CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR = 'div.flex.w-full.items-center.justify-between:has(span:contains("Customer Service")) span.e1xzmg0z.sr2r3oj'
FEATURES_TOTAL_RATING_SELECTOR = 'div.flex.w-full.items-center.justify-between:has(span:contains("Features")) span.e1xzmg0z.sr2r3oj' # Assuming "Functionality" maps to "Features"
VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR = 'div.flex.w-full.items-center.justify-between:has(span:contains("Value for Money")) span.e1xzmg0z.sr2r3oj'
PRODUCT_OVERALL_RATING_HEADER_SELECTOR = 'div[class*="sticky top-0"] div[class*="s1ncqr9d"] span[class*="sr2r3oj"]' # For main product rating e.g. "4.7 (32)"
REVIEW_COUNT_DISPLAY_SELECTOR = 'span.typo-30.font-semibold:contains("Reviews")' # e.g. "Showing 25 of 32 Reviews"


# --- Review Card Internal Selectors (based on capterra.html structure) ---
REVIEWER_NAME_SELECTOR = 'span.typo-20.text-neutral-99.font-semibold'
REVIEWER_DETAILS_BLOCK_SELECTOR = REVIEWER_NAME_SELECTOR + ' + br + span.text-neutral-90, ' + REVIEWER_NAME_SELECTOR + ' ~ div.text-neutral-90' # Heuristic
# A better approach for reviewer details: Select the parent div containing name and other details
REVIEWER_INFO_CONTAINER_SELECTOR = 'div.typo-10.text-neutral-90.w-full.lg\\:w-fit' # This one seems to hold all text
REVIEWER_AVATAR_IMG_SELECTOR = 'img[data-testid="reviewer-profile-pic"]'
REVIEWER_INITIALS_FALLBACK_SELECTOR = 'div.e1xzmg0z.ajdk2qt.bg-primary-20' # If no avatar img

REVIEW_TITLE_SELECTOR = 'h3.typo-20.font-semibold'
REVIEW_DATE_PUBLISHED_SELECTOR = 'div.space-y-1 + div.typo-0.text-neutral-90' # Date usually after title block
REVIEW_CARD_OVERALL_RATING_SELECTOR = 'div[data-testid="rating"] span.e1xzmg0z.sr2r3oj' # Rating for THIS review
REVIEW_PROS_SELECTOR = 'div.space-y-2:has(svg title:contains("Positive icon")) p'
REVIEW_CONS_SELECTOR = 'div.space-y-2:has(svg title:contains("Negative icon")) p'
# REVIEW_TEXT_SELECTOR: Overall comment, typically the first <p> in the main text body after ratings/title, not pros/cons
REVIEW_TEXT_SELECTOR = 'div[class*="!mt-4 space-y-6"] > p:first-child'


POPUP_CLOSE_SELECTORS_CAPTERRA = [
    "#onetrust-accept-btn-handler",
    'div[data-modal-role="overlay"] + div i[data-modal-role="close-button"][class*="modal-close"]', # For the "Send me a list" popup
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
    # # options.add_argument("--disable-gpu") # Usually not needed if not headless

    # # Keep these minimal, as too many can be a fingerprint
    # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # options.add_experimental_option('useAutomationExtension', False)
    # # options.add_argument("--disable-blink-features=AutomationControlled") # Can sometimes cause issues

    # user_agent_str = ua.random if ua else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    # options.add_argument(f'user-agent={user_agent_str}')
    # options.add_argument('--log-level=3')
    # options.add_argument("--start-maximized") 
    # options.add_argument("--lang=en-US,en;q=0.9") 

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
        time.sleep(random.uniform(0.5, 0.8)) # Reduced scroll pause
        # Try JS click first for popups as it's often more direct
        if "popup" in thread_name.lower() or "close" in thread_name.lower(): # Heuristic for popup clicks
             driver.execute_script("arguments[0].click();", element)
        else:
            element.click() # Standard click for other elements like "Show More"
        return True
    except ElementClickInterceptedException:
        print(f"    [{thread_name}][try_click] Click intercepted. Trying JS click after short delay.")
        time.sleep(random.uniform(0.2, 0.4))
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

    # Prioritize specific popup first if known
    specific_popup_selector = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"]'
    try:
        specific_popups = driver.find_elements(By.CSS_SELECTOR, specific_popup_selector)
        for sp_btn in specific_popups:
            if sp_btn.is_displayed() and sp_btn.is_enabled():
                print(f"      [{thread_name}] Attempting to close SPECIFIC 'Send me a list' popup...")
                if try_click(driver, sp_btn, SELENIUM_INTERACTION_TIMEOUT_S / 2, f"{thread_name}-SpecificPopup"):
                    closed_any = True; time.sleep(0.7)
                    print(f"      [{thread_name}] Specific popup likely closed.")
                    break # Assume only one of these
    except Exception as e_spec:
        print(f"      [{thread_name}] Error checking specific popup: {e_spec}")


    for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS_CAPTERRA):
        if sel == specific_popup_selector and closed_any: continue # Already tried

        current_closed_this_selector = False
        # Check in iframes first (common for cookie banners)
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe_idx, iframe in enumerate(iframes):
            try:
                if not iframe.is_displayed(): continue
                driver.switch_to.frame(iframe)
                popups_in_iframe = driver.find_elements(By.CSS_SELECTOR, sel)
                for popup_btn_iframe in popups_in_iframe:
                    if popup_btn_iframe.is_displayed() and popup_btn_iframe.is_enabled():
                        print(f"          [{thread_name}] Found popup in iframe (selector: {sel[:30]})")
                        if try_click(driver, popup_btn_iframe, SELENIUM_INTERACTION_TIMEOUT_S / 2, f"{thread_name}-IframePopup"):
                            closed_any = True; current_closed_this_selector = True; time.sleep(0.7)
                            break 
                driver.switch_to.default_content()
                if current_closed_this_selector: break 
            except Exception: driver.switch_to.default_content()
            if current_closed_this_selector: break
        if current_closed_this_selector: continue

        try: # Check in main content
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            if not popups: continue
            for popup_btn in popups:
                if popup_btn.is_displayed() and popup_btn.is_enabled():
                    print(f"      [{thread_name}] Attempting main content popup (selector: {sel[:30]})")
                    if try_click(driver, popup_btn, SELENIUM_INTERACTION_TIMEOUT_S / 2, f"{thread_name}-MainPopup"):
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


def parse_capterra_datetime(date_str: str) -> Tuple[Optional[str], Optional[datetime]]:
    """Parses Capterra's date string (e.g., "June 1, 2022") into a desired output string and datetime object."""
    if not date_str:
        return None, None
    
    # Capterra's on-page format
    parsed_dt = None
    formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try:
            parsed_dt = datetime.strptime(date_str.strip(), fmt)
            break
        except ValueError:
            continue
    
    if parsed_dt:
        # Format for desired output JSON: "2022-06-01 06:18:16 -0400" (example, actual time not on page)
        # We'll just use the date part and a dummy time/timezone for the string.
        # The datetime_obj will be just the date.
        output_str = parsed_dt.strftime("%Y-%m-%d") + " 00:00:00 +0000" # Dummy time and UTC
        return output_str, parsed_dt
    else:
        # print(f"      [Date Parse] Warning: Could not parse Capterra date: {date_str}")
        return date_str, None # Return original string if parsing fails


def _parse_individual_review_card_revised(review_card_soup: BeautifulSoup, product_url: str, thread_name: str) -> Optional[CapterraIndividualReview]:
    try:
        # Reviewer Name and Avatar
        reviewer_name_el = review_card_soup.select_one(REVIEWER_NAME_SELECTOR)
        reviewer_name = reviewer_name_el.get_text(strip=True) if reviewer_name_el else None
        
        avatar_el = review_card_soup.select_one(REVIEWER_AVATAR_IMG_SELECTOR)
        reviewer_avatar_url = avatar_el['src'] if avatar_el and avatar_el.has_attr('src') else None
        if not reviewer_avatar_url: # Fallback to initials if no image
            initials_el = review_card_soup.select_one(REVIEWER_INITIALS_FALLBACK_SELECTOR)
            if initials_el and not reviewer_name: # If name is also missing, use initials as name
                 reviewer_name = initials_el.get_text(strip=True)


        # Title
        title_el = review_card_soup.select_one(REVIEW_TITLE_SELECTOR)
        title = title_el.get_text(strip=True) if title_el else "No Title"

        # Date
        date_str_on_page = "Unknown Date"
        date_el_candidates = review_card_soup.select(REVIEW_DATE_PUBLISHED_SELECTOR)
        if date_el_candidates: date_str_on_page = date_el_candidates[0].get_text(strip=True)
        
        datetime_str_output, datetime_obj_parsed = parse_capterra_datetime(date_str_on_page)

        # Overall Rating for this review
        rating_el = review_card_soup.select_one(REVIEW_CARD_OVERALL_RATING_SELECTOR)
        rating_str = rating_el.get_text(strip=True) if rating_el else "0.0"
        
        # Pros
        pros_el = review_card_soup.select_one(REVIEW_PROS_SELECTOR)
        pros = pros_el.get_text(strip=True) if pros_el else None
        
        # Cons
        cons_el = review_card_soup.select_one(REVIEW_CONS_SELECTOR)
        cons = cons_el.get_text(strip=True) if cons_el else None

        # Overall Review Text (text field)
        # This is tricky: it's often the first <p> in the main body, but NOT pros or cons.
        review_text = ""
        review_text_candidates = review_card_soup.select(REVIEW_TEXT_SELECTOR)
        if review_text_candidates:
            candidate_text = review_text_candidates[0].get_text(strip=True)
            # Ensure it's not the same as pros or cons if those selectors are too general
            if candidate_text != pros and candidate_text != cons:
                review_text = candidate_text

        # Review ID and URL (placeholders, Capterra doesn't expose these easily per review card)
        review_id = None # Could hash content later if needed for uniqueness
        review_url = None 

        return CapterraIndividualReview(
            title=title,
            text=review_text,
            reviewer=reviewer_name,
            reviewer_avatar=reviewer_avatar_url,
            id=review_id,
            datetime_str=datetime_str_output,
            datetime_obj=datetime_obj_parsed,
            rating=rating_str,
            url=review_url,
            pros=pros,
            cons=cons
        )
    except Exception as e:
        print(f"      [{thread_name}] Error parsing individual review card: {type(e).__name__} - {e}")
        traceback.print_exc(file=sys.stdout)
        return None

def _parse_capterra_html_for_reviews(
    page_source: str, product_url_str: str, product_name_header: str, product_category_header: Optional[str],
    start_date_filter: Optional[datetime], end_date_filter: Optional[datetime],
    thread_name: str
) -> CapterraScrapeResultOutput:
    
    soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER)
    parsed_reviews: List[CapterraIndividualReview] = []
    
    # --- Parse Totals ---
    overall_rating_str, ease_of_use_str, customer_service_str, features_str, value_money_str = None, None, None, None, None
    review_count_from_display = None

    # Get overall product rating from header (e.g., "4.7 (32)")
    # This is different from individual review ratings
    overall_product_rating_el = soup.select_one(PRODUCT_OVERALL_RATING_HEADER_SELECTOR)
    if overall_product_rating_el:
        match = re.match(r"([\d\.]+)(?:\s*\((\d+)\))?", overall_product_rating_el.get_text(strip=True))
        if match:
            overall_rating_str = match.group(1)
            if match.group(2): # If total count is with overall rating
                 review_count_from_display = int(match.group(2))


    # Attempt to get total review count if not found with overall rating
    if not review_count_from_display:
        review_count_display_el = soup.select_one(REVIEW_COUNT_DISPLAY_SELECTOR)
        if review_count_display_el:
            count_match = re.search(r"of\s+(\d+)\s+Reviews", review_count_display_el.get_text())
            if count_match: review_count_from_display = int(count_match.group(1))

    # Parse detailed total ratings if available (these selectors are speculative)
    summary_section = soup.select_one(PRODUCT_RATING_SUMMARY_SECTION_SELECTOR)
    if summary_section:
        ease_el = summary_section.select_one(EASE_OF_USE_TOTAL_RATING_SELECTOR)
        if ease_el: ease_of_use_str = ease_el.get_text(strip=True).split()[0]

        cust_el = summary_section.select_one(CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR)
        if cust_el: customer_service_str = cust_el.get_text(strip=True).split()[0]
        
        feat_el = summary_section.select_one(FEATURES_TOTAL_RATING_SELECTOR)
        if feat_el: features_str = feat_el.get_text(strip=True).split()[0]

        val_el = summary_section.select_one(VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR)
        if val_el: value_money_str = val_el.get_text(strip=True).split()[0]
    
    totals = CapterraReviewTotals(
        review_count=review_count_from_display,
        overall_rating=overall_rating_str,
        ease_of_use_rating=ease_of_use_str,
        customer_service_rating=customer_service_str,
        functionality_rating=features_str, # Mapping 'Features' to 'functionality_rating'
        value_for_money_rating=value_money_str
    )
    
    # --- Parse Individual Reviews ---
    review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)
    if review_cards_container:
        review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR)
        print(f"  [{thread_name}] Found {len(review_card_soups)} review card elements in HTML for parsing.")
        
        for idx, card_soup in enumerate(review_card_soups):
            review = _parse_individual_review_card_revised(card_soup, product_url_str, thread_name)
            if review:
                # Date filtering (if datetime_obj was successfully parsed)
                if start_date_filter and review.datetime_obj and review.datetime_obj < start_date_filter:
                    continue
                if end_date_filter and review.datetime_obj and review.datetime_obj > end_date_filter:
                    continue
                parsed_reviews.append(review)
    else:
        print(f"  [{thread_name}] CRITICAL: Review cards container ('{REVIEW_CARDS_CONTAINER_SELECTOR}') not found in final page source.")

    return CapterraScrapeResultOutput(
        totals=totals,
        reviews=parsed_reviews,
        product_name_scraped=product_name_header,
        product_category_scraped=product_category_header,
        original_url=HttpUrl(product_url_str),
        reviews_count_scraped=len(parsed_reviews),
        scrape_duration_seconds=0 # Will be set by caller
    )


def _scrape_capterra_page_for_reviews( # Renamed from _scrape_capterra_reviews_for_url
    product_url_str: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Optional[CapterraScrapeResultOutput]: # Changed return type
    thread_name = f"CapterraLoad-{company_slug[:15]}"
    scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started Selenium phase for: {product_url_str}")
    
    driver = None
    product_name_from_header = "Unknown Product" # For final output if parsing fails earlier
    category_name_from_header = None

    try:
        driver = setup_selenium_driver()
        print(f"  [{thread_name}] Navigating to {product_url_str}...")
        driver.get(product_url_str)
        print(f"  [{thread_name}] Page loaded. Sleeping for {INITIAL_PAGE_LOAD_SLEEP_S:.2f}s for dynamic content...")
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        attempt_to_close_popups_capterra(driver, thread_name)

        print(f"  [{thread_name}] Attempting to extract initial product name and category from header...")
        try:
            # Wait for a general header area
            WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="sticky top-0"]'))
            )
            try: # Product name from H1 "Reviews of X"
                h1_el = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, PRODUCT_NAME_HEADER_FALLBACK_H1_SELECTOR)))
                if "reviews of" in h1_el.text.lower():
                    product_name_from_header = h1_el.text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
            except TimeoutException: 
                print(f"    [{thread_name}] Fallback H1 product name selector timed out.")
                # Try the original span selector if H1 failed
                try:
                    product_name_el = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR_HEADER)))
                    product_name_from_header = product_name_el.text.strip()
                except:
                    print(f"    [{thread_name}] Original span product name selector also failed.")
                    product_name_from_header = company_slug.replace("-"," ").title() # Last resort

            print(f"    [{thread_name}] Product Name tentatively: {product_name_from_header}")
            try: 
                cat_el = driver.find_element(By.CSS_SELECTOR, PRODUCT_CATEGORY_BREADCRUMB_SELECTOR)
                category_name_from_header = cat_el.text.strip()
                print(f"    [{thread_name}] Product Category: {category_name_from_header}")
            except: print(f"    [{thread_name}] Category breadcrumb not found.")
        except Exception as e_header:
            print(f"  [{thread_name}] Issue extracting header info: {e_header}")


        print(f"  [{thread_name}] Starting 'Show more reviews' loop...")
        show_more_clicks = 0
        consecutive_no_new_reviews_dom = 0

        while True: # Loop indefinitely until button is gone or no new content
            num_reviews_before_click_dom = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
            
            try:
                # Wait for the container of reviews to ensure page is responsive
                WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR))
                )
                
                show_more_button = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S / 2.5 ).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR))
                )
                
                if not show_more_button.is_enabled():
                    print(f"  [{thread_name}] 'Show more' button found but not enabled. Assuming end.")
                    break
                
                print(f"  [{thread_name}] Clicking 'Show more reviews' (Click #{show_more_clicks + 1}). DOM reviews: {num_reviews_before_click_dom}")
                
                # Scroll slightly above the button to ensure it's not obscured by sticky footers/headers
                driver.execute_script("arguments[0].scrollIntoView(true); window.scrollBy(0, -150);", show_more_button)
                time.sleep(random.uniform(0.5, 1.0)) 
                
                WebDriverWait(driver, SELENIUM_INTERACTION_TIMEOUT_S).until(EC.element_to_be_clickable(show_more_button))
                
                try: # Attempt JS click first
                    driver.execute_script("arguments[0].click();", show_more_button)
                except Exception as e_js_click:
                    print(f"    [{thread_name}] JS click failed for 'Show More': {e_js_click}. Trying Selenium click.")
                    show_more_button.click() # Fallback
                
                show_more_clicks += 1
                print(f"    [{thread_name}] Clicked. Sleeping for {AFTER_SHOW_MORE_CLICK_SLEEP_S:.2f}s...")
                time.sleep(AFTER_SHOW_MORE_CLICK_SLEEP_S) # Essential for new content to load
                
                # Check if any overlays reappeared
                attempt_to_close_popups_capterra(driver, thread_name)


                num_reviews_after_click_dom = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
                if num_reviews_after_click_dom == num_reviews_before_click_dom:
                    consecutive_no_new_reviews_dom += 1
                    print(f"    [{thread_name}] DOM review count did not increase. Consecutive: {consecutive_no_new_reviews_dom}")
                    if consecutive_no_new_reviews_dom >= 3:
                        print(f"  [{thread_name}] DOM review count stable for {consecutive_no_new_reviews_dom} clicks. Assuming end.")
                        break
                else:
                    consecutive_no_new_reviews_dom = 0

            except TimeoutException:
                print(f"  [{thread_name}] 'Show more reviews' button timed out (not found/visible/clickable). Assuming all reviews are loaded.")
                break
            except Exception as e_sm:
                print(f"  [{thread_name}] Error in 'Show more' loop: {type(e_sm).__name__} - {e_sm}")
                traceback.print_exc(file=sys.stdout)
                attempt_to_close_popups_capterra(driver, thread_name) # Try to clear overlays on error
                time.sleep(1) # Pause and hope it recovers or breaks next iteration
        
        print(f"  [{thread_name}] Finished 'Show more' loop. Total clicks: {show_more_clicks}.")
        print(f"  [{thread_name}] Retrieving final page source...")
        final_page_source = driver.page_source
        
        print(f"  [{thread_name}] Quitting Selenium driver.")
        driver.quit()
        driver = None 
        
        print(f"  [{thread_name}] Starting HTML parsing phase...")
        parsed_data = _parse_capterra_html_for_reviews(
            final_page_source, product_url_str, product_name_from_header, category_name_from_header,
            start_date_filter, end_date_filter, thread_name
        )
        parsed_data.scrape_duration_seconds = round(time.perf_counter() - scrape_start_time, 2)
        
        print(f"  [{thread_name}] Finished all phases for {product_url_str} in {parsed_data.scrape_duration_seconds:.2f}s. Scraped {parsed_data.reviews_count_scraped} reviews.")
        return parsed_data

    except Exception as e_main_scrape:
        print(f"  [{thread_name}] MAJOR ERROR for {product_url_str}: {type(e_main_scrape).__name__} - {e_main_scrape}")
        traceback.print_exc()
        return None
    finally:
        if driver: driver.quit(); print(f"  [{thread_name}] WebDriver quit in final exception handler.")


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
            "data": result_obj.model_dump(mode='json', by_alias=True), # by_alias for datetime_str
            "summary": {
                "product_name": result_obj.product_name_scraped,
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
    
    with ThreadPoolExecutor(max_workers=min(len(request.urls), 2)) as executor: # Reduced max_workers for stability
        future_to_url = {}
        for url_obj in request.urls:
            url_str = str(url_obj)
            if "capterra.com/p/" not in url_str or "/reviews" not in url_str:
                 results[url_str] = {"status": "error", "message": f"Invalid Capterra URL format: {url_str}."}
                 continue
            future = executor.submit(scrape_capterra_sync, url_str, start_date_filter, end_date_filter)
            future_to_url[future] = url_str

        for future in as_completed(future_to_url):
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

if __name__ == "__main__":
    async def main_test_capterra():
        test_url_scholar = "https://www.capterra.com/p/135005/Scholar-LMS/reviews/"
        # test_url_google = "https://www.capterra.com/p/253176/Google-One/reviews/"
        
        test_request = ScrapeRequest(urls=[HttpUrl(test_url_scholar)])
        
        results = await scrape_capterra_endpoint(test_request)
        print(json.dumps(results, indent=2, default=str)) # default=str for datetime objects
    asyncio.run(main_test_capterra())
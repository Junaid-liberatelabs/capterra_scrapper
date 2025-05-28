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

class ReviewerInfo(BaseModel):
    name: Optional[str] = None
    initials: Optional[str] = None
    profile_pic_url: Optional[HttpUrl] = None
    job_title: Optional[str] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None # e.g. "11 - 50 employees"
    time_used_product: Optional[str] = None
    is_verified_linkedin: bool = False # Placeholder, Capterra seems to have direct "Verified User" or LinkedIn icon

class RatingDetail(BaseModel):
    category: str # e.g., "Overall Rating", "Ease of Use", "Customer Service", "Features", "Value for Money", "Likelihood to Recommend"
    rating: float
    max_rating: Optional[int] = 5 # For stars, typically 5. For Likelihood to Recommend, it's 10.

class ProductLink(BaseModel):
    name: str
    logo_url: Optional[HttpUrl] = None
    url: Optional[HttpUrl] = None

class CapterraReview(BaseModel):
    review_id: Optional[str] = None # Could be useful, maybe derive from a unique part of the card
    reviewer: ReviewerInfo
    title: Optional[str] = None
    date_published_str: str # Keep as string initially, convert later
    date_published: Optional[datetime] = None
    overall_rating: float # The main 5-star rating for this review
    rating_details: List[RatingDetail] = []
    overall_comment: Optional[str] = None # Main review body, if distinct
    pros: Optional[str] = None
    cons: Optional[str] = None
    alternatives_considered: List[ProductLink] = []
    reason_for_choosing: Optional[str] = None
    switched_from: List[ProductLink] = []
    reason_for_switching: Optional[str] = None
    source_type: Optional[str] = None # e.g., "Incentivized review"
    source_tooltip: Optional[str] = None

class ProductInfo(BaseModel):
    name: str
    capterra_url: HttpUrl
    logo_url: Optional[HttpUrl] = None
    overall_product_rating: Optional[float] = None
    total_product_reviews_count_header: Optional[int] = None # From the header section
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

# --- FastAPI ---
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Capterra Scraper API",
    description="Selenium-based scraper for Capterra.com product reviews.",
    version="1.0.0"
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
# Curl-CFFI likely not needed for Capterra's "Show More" mechanism

try: from fake_useragent import UserAgent; ua = UserAgent()
except ImportError: print("Warning: fake-useragent not installed."); ua = None

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants for Capterra ---
SELENIUM_PAGE_TIMEOUT_S = 30
SELENIUM_ELEMENT_TIMEOUT_S = 15
SELENIUM_INTERACTION_TIMEOUT_S = 8
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(2.0, 3.5) # Capterra might be heavier
AFTER_SHOW_MORE_CLICK_SLEEP_S = random.uniform(1.5, 2.5) # Allow time for new reviews to render

# Selectors for Capterra
SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
# This is a bit broad, relies on being inside REVIEW_CARDS_CONTAINER_SELECTOR
INDIVIDUAL_REVIEW_CARD_SELECTOR = '> div.e1xzmg0z.c1ofrhif.typo-10' # Direct child div of the container

# Product Info Selectors (Header)
PRODUCT_NAME_SELECTOR_HEADER = 'span.e1xzmg0z.h11hhycw.font-semibold' # Appears to be the main product name
PRODUCT_LOGO_SELECTOR_HEADER = 'figure.e1xzmg0z.tf6i4tz img'
PRODUCT_RATING_TEXT_SELECTOR_HEADER = 'div.flex.items-center.gap-1.hidden.lg:flex span.e1xzmg0z.sr2r3oj' # e.g., "4.7 (32)"
PRODUCT_CATEGORY_BREADCRUMB_SELECTOR = 'nav[class*="be9etqu"] a[data-testid="categoryslug"]'

# Review Card Internal Selectors
REVIEWER_PROFILE_PIC_SELECTOR = 'img[data-testid="reviewer-profile-pic"]'
REVIEWER_INITIALS_SELECTOR = 'div.e1xzmg0z.ajdk2qt.bg-primary-20' # Get text if no img
REVIEWER_NAME_SELECTOR = 'span.typo-20.text-neutral-99.font-semibold'
REVIEWER_DETAILS_BLOCK_SELECTOR = 'div.typo-10.text-neutral-90.w-full.lg\\:w-fit' # Contains Name, Job, Industry, Time Used
REVIEW_TITLE_SELECTOR = 'h3.typo-20.font-semibold'
REVIEW_DATE_SELECTOR = 'div.typo-0.text-neutral-90' # Sibling/near title
REVIEW_OVERALL_RATING_VALUE_SELECTOR = 'div[data-testid="rating"] span.e1xzmg0z.sr2r3oj'
# Detailed ratings are inside a dropdown structure, but seem to be in HTML
REVIEW_RATINGS_DROPDOWN_CONTAINER_SELECTOR = 'div[role="dialog"].e1xzmg0z.c1ghu4k7.l1ix9ysh'
REVIEW_RATING_CATEGORY_ITEM_SELECTOR = REVIEW_RATINGS_DROPDOWN_CONTAINER_SELECTOR + ' div.typo-20.text-neutral-99.flex.cursor-pointer'
REVIEW_RATING_CATEGORY_NAME_SELECTOR = 'span.text-neutral-95.whitespace-nowrap'
REVIEW_RATING_CATEGORY_VALUE_SELECTOR = 'div[data-testid*="-rating"] span.e1xzmg0z.sr2r3oj' # e.g. data-testid="Ease of Use-rating"

REVIEW_OVERALL_COMMENT_SELECTOR = 'div.\\!mt-4.space-y-6 > p' # This might need adjustment if multiple <p> exist
REVIEW_PROS_TEXT_SELECTOR = 'div.space-y-2:has(svg title:contains("Positive icon")) p'
REVIEW_CONS_TEXT_SELECTOR = 'div.space-y-2:has(svg title:contains("Negative icon")) p'

REVIEW_SOURCE_GROUP_SELECTOR = 'div[role="group"][aria-labelledby="review-source-label"]'
REVIEW_SOURCE_TOOLTIP_SELECTOR = REVIEW_SOURCE_GROUP_SELECTOR + ' + div[role="dialog"]' # Tooltip text for source

# Alternatives/Switched From Selectors
REVIEW_ALTERNATIVES_SECTION_SELECTOR = 'div.space-y-4:has(span:contains("Alternatives considered"))'
REVIEW_SWITCHED_FROM_SECTION_SELECTOR = 'div.space-y-4:has(span:contains("Switched from"))'
REVIEW_PRODUCT_LINK_SELECTOR = 'a.e1xzmg0z.ljas29s.flex.items-center.gap-x-2'
REVIEW_PRODUCT_LINK_IMG_SELECTOR = 'figure img'
REVIEW_PRODUCT_LINK_NAME_SELECTOR = 'span.typo-10.whitespace-nowrap.font-normal'
REVIEW_REASON_SELECTOR = 'p' # usually the <p> after the product links div

POPUP_CLOSE_SELECTORS_CAPTERRA = [
    "#onetrust-accept-btn-handler", # Cookie banner
    "button[aria-label='close' i]",
    "button[class*='modal__close' i]",
    "button[aria-label*='Dismiss' i]",
    "div[role='dialog'] button[class*='close']",
]

# --- Helper Functions ---
# ( Reuse setup_selenium_driver, try_click, adapt attempt_to_close_popups)

def setup_selenium_driver() -> webdriver.Chrome:
    # ... (Same as in v15.py, ensure headless can be toggled for debugging) ...
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Enable for production
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--blink-settings=imagesEnabled=false") # Faster loading

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("window-size=1920,1080")
    user_agent_str = ua.random if ua else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')
    options.add_argument('--log-level=3')

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        return driver
    except Exception as e:
        print(f"  [Selenium Setup] CRITICAL ERROR: {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup Selenium driver: {e}")

def try_click(driver: webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S):
    # ... (Largely same as in v15.py, ensure robust error logging) ...
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
        # Scroll element into view using JavaScript
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", element)
        time.sleep(0.3 + random.uniform(0.1, 0.3)) # Brief pause after scroll
        element.click()
        return True
    except ElementClickInterceptedException:
        print(f"    [try_click] Click intercepted. Trying JS click.")
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e_js:
            print(f"    [try_click] JS click failed: {e_js}")
            return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException) as e_click:
        error_message = str(e_click).splitlines()[0] if str(e_click) else ""
        print(f"    [try_click] Click failed: {type(e_click).__name__} - {error_message}")
        return False
    except Exception as e_other_click:
        print(f"    [try_click] Unexpected click error: {type(e_other_click).__name__} - {e_other_click}")
        return False


def attempt_to_close_popups_capterra(driver: webdriver.Chrome, thread_name: str):
    # ... (Adapt from v15.py using POPUP_CLOSE_SELECTORS_CAPTERRA) ...
    closed_any = False
    main_window = driver.current_window_handle
    initial_handles = set(driver.window_handles)

    for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS_CAPTERRA):
        try:
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            if not popups:
                continue
            for popup_btn in popups:
                if popup_btn.is_displayed() and popup_btn.is_enabled():
                    print(f"      [{thread_name}] Attempting Capterra popup close ({sel_idx+1}/{len(POPUP_CLOSE_SELECTORS_CAPTERRA)}) with: {sel[:50]}...")
                    try:
                        driver.execute_script("arguments[0].click();", popup_btn) # JS click often more robust for overlays
                        time.sleep(0.5 + random.uniform(0.1, 0.3)) # Wait for popup to disappear
                        closed_any = True
                        # Check if the button is gone or not displayed
                        if not popup_btn.is_displayed():
                            print(f"      [{thread_name}] Popup seems closed ({sel[:50]}...).")
                            break # Break from inner loop (popups for this selector)
                    except StaleElementReferenceException:
                        print(f"      [{thread_name}] Popup closed (stale element - {sel[:50]}...).")
                        closed_any = True
                        break
                    except Exception as e_close:
                        print(f"      [{thread_name}] Error clicking popup close ({sel[:50]}...): {e_close}")
            if closed_any and (not popups or not popups[0].is_displayed()): # If closed one and the first is gone, assume this type is handled
                 break
        except Exception as e_find_popup:
            # print(f"      [{thread_name}] Error finding popups with selector {sel[:50]}: {e_find_popup}")
            pass # Silently continue if selector not found or other minor issues

    # Handle new windows/tabs if any opened
    final_handles = set(driver.window_handles)
    new_handles = final_handles - initial_handles
    if new_handles:
        for handle in new_handles:
            if handle != main_window:
                try:
                    print(f"      [{thread_name}] Closing unexpected new window/tab.")
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception as e_win_close:
                    print(f"      [{thread_name}] Error closing new window: {e_win_close}")
        driver.switch_to.window(main_window)

    if closed_any:
        time.sleep(0.5 + random.uniform(0.1, 0.3)) # Brief pause if any popup was actioned

def parse_date_capterra(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    # Capterra dates like "September 16, 2022"
    # More formats might be needed if the site localizes dates heavily
    formats_to_try = ["%B %d, %Y", "%b %d, %Y"]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    print(f"      [Date Parse] Warning: Could not parse date: {date_str}")
    return None


# --- Main Capterra Scraper Function ---
def _parse_individual_review_card(review_card_soup: BeautifulSoup, base_url: str, thread_name: str) -> Optional[CapterraReview]:
    try:
        # Reviewer Info
        reviewer_name_el = review_card_soup.select_one(REVIEWER_NAME_SELECTOR)
        reviewer_name = reviewer_name_el.get_text(strip=True) if reviewer_name_el else None

        reviewer_details_block = review_card_soup.select_one(REVIEWER_DETAILS_BLOCK_SELECTOR)
        job_title, industry, company_size_text, time_used = None, None, None, None
        if reviewer_details_block:
            # Details are typically separated by <br>. Name is the first part.
            # The structure is Name<br>Job<br>Industry<br>TimeUsed
            # We need to get text nodes and filter out the name if already captured by REVIEWER_NAME_SELECTOR
            # This part is tricky because structure can vary (e.g. "Verified Reviewer" has fewer lines)
            details_parts = [
                s.strip() for s in reviewer_details_block.find_all(string=True, recursive=False) if s.strip() and s.strip() != reviewer_name
            ]
            # Alternative: split by <br> tags
            br_separated_parts = [part.strip() for part in str(reviewer_details_block).split('<br>') if part.strip()]
            cleaned_parts = []
            for part_html in br_separated_parts:
                part_soup = BeautifulSoup(f"<div>{part_html}</div>", DEFAULT_HTML_PARSER)
                text_content = part_soup.get_text(strip=True)
                if text_content and text_content != reviewer_name:
                    cleaned_parts.append(text_content)

            if reviewer_name == "Verified Reviewer" and len(cleaned_parts) >= 2: # Job, Industry, Time Used
                job_title = cleaned_parts[0] if len(cleaned_parts) > 0 else None
                industry = cleaned_parts[1] if len(cleaned_parts) > 1 else None
                # Company size often part of industry or a separate line. For "Verified Reviewer", it might be missing.
                # Time used
                time_used_search = [s for s in cleaned_parts if "used the software for:" in s.lower()]
                if time_used_search:
                    time_used = time_used_search[0].replace("Used the software for:", "").strip()

            elif len(cleaned_parts) >= 3: # Name, Job, Industry, Time
                job_title = cleaned_parts[0] if len(cleaned_parts) > 0 else None # First element after name
                industry = cleaned_parts[1] if len(cleaned_parts) > 1 else None
                # Company size might be part of industry string or a separate line
                time_used_search = [s for s in cleaned_parts if "used the software for:" in s.lower()]
                if time_used_search:
                    time_used = time_used_search[0].replace("Used the software for:", "").strip()
                else: # Check if time used is the last element
                    if len(cleaned_parts) > 2 and "used the software for:" not in cleaned_parts[-1].lower(): # Heuristic
                        pass # Time used might be missing or in a different format
                    elif len(cleaned_parts) > 2 : time_used = cleaned_parts[-1]


        profile_pic_el = review_card_soup.select_one(REVIEWER_PROFILE_PIC_SELECTOR)
        profile_pic_url = profile_pic_el['src'] if profile_pic_el and profile_pic_el.has_attr('src') else None
        initials = None
        if not profile_pic_url:
            initials_el = review_card_soup.select_one(REVIEWER_INITIALS_SELECTOR)
            if initials_el: initials = initials_el.get_text(strip=True)

        # Check for LinkedIn verification - this is a placeholder logic, actual icon needs specific check
        is_verified_linkedin = bool(review_card_soup.select_one('i[class*="icon-linkedin"]')) # Example
        if not reviewer_name and initials: # Common for "Verified Reviewer"
            reviewer_name = "Verified Reviewer" if "VR" in initials else initials


        reviewer_info = ReviewerInfo(
            name=reviewer_name, initials=initials, profile_pic_url=profile_pic_url,
            job_title=job_title, industry=industry, company_size=company_size_text, # TODO: Parse company size from industry string if needed
            time_used_product=time_used,
            is_verified_linkedin=is_verified_linkedin
        )

        title_el = review_card_soup.select_one(REVIEW_TITLE_SELECTOR)
        title = title_el.get_text(strip=True) if title_el else None

        # Date - often near title. Need to be specific.
        # Assuming date is in a div.typo-0.text-neutral-90 that is a sibling or near child of title's parent
        date_el = None
        if title_el:
            parent_of_title = title_el.parent
            date_candidates = parent_of_title.select(REVIEW_DATE_SELECTOR)
            if date_candidates: date_el = date_candidates[0] # take first
        if not date_el: # Fallback: search wider
             date_el = review_card_soup.select_one(f"{REVIEW_DATE_SELECTOR}") # More global search in card
        
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
                cat_val_el = item.select_one(REVIEW_RATING_CATEGORY_VALUE_SELECTOR) # This selector might need to be more specific to the item
                if not cat_val_el: # Try a more specific selector within the item if general one fails
                    cat_val_el = item.select_one('span.e1xzmg0z.sr2r3oj')

                if cat_name_el and cat_val_el:
                    cat_name = cat_name_el.get_text(strip=True)
                    cat_val_text = cat_val_el.get_text(strip=True).replace("/10","") # For likelihood to recommend
                    try:
                        cat_val = float(cat_val_text)
                        max_r = 10 if "/10" in cat_val_el.get_text(strip=True) else 5
                        rating_details_list.append(RatingDetail(category=cat_name, rating=cat_val, max_rating=max_r))
                    except ValueError:
                        print(f"      [{thread_name}] Warning: Could not parse rating value '{cat_val_text}' for category '{cat_name}'")

        # Pros and Cons
        pros_el = review_card_soup.select_one(REVIEW_PROS_TEXT_SELECTOR)
        pros_text = pros_el.get_text(strip=True) if pros_el else None
        cons_el = review_card_soup.select_one(REVIEW_CONS_TEXT_SELECTOR)
        cons_text = cons_el.get_text(strip=True) if cons_el else None

        # Overall comment (if separate from pros/cons)
        # This is heuristic: often a <p> directly under a specific div after title/date block
        overall_comment_el = review_card_soup.select_one(REVIEW_OVERALL_COMMENT_SELECTOR)
        overall_comment_text = None
        if overall_comment_el:
            # Ensure it's not pros or cons text if selectors are similar
            temp_overall_text = overall_comment_el.get_text(strip=True)
            if temp_overall_text != pros_text and temp_overall_text != cons_text:
                overall_comment_text = temp_overall_text
        
        source_type, source_tooltip = None, None
        source_group = review_card_soup.select_one(REVIEW_SOURCE_GROUP_SELECTOR)
        if source_group:
            source_type = source_group.get_text(strip=True).replace("Review Source","").strip() # Basic text
            tooltip_el = source_group.find_next_sibling('div', role='dialog') # Assumes tooltip is next sibling
            if tooltip_el:
                source_tooltip = tooltip_el.get_text(strip=True)


        # Alternatives / Switched From (These are complex due to varying presence)
        # Simplified for now, would need robust conditional parsing
        alternatives_considered, reason_for_choosing = [], None
        switched_from, reason_for_switching = [], None

        # Try to parse "Switched from" section
        switched_section = review_card_soup.select_one(REVIEW_SWITCHED_FROM_SECTION_SELECTOR)
        if switched_section:
            product_links_els = switched_section.select(REVIEW_PRODUCT_LINK_SELECTOR)
            for link_el in product_links_els:
                name_el = link_el.select_one(REVIEW_PRODUCT_LINK_NAME_SELECTOR)
                name = name_el.get_text(strip=True) if name_el else "Unknown Product"
                logo_el = link_el.select_one(REVIEW_PRODUCT_LINK_IMG_SELECTOR)
                logo_url = logo_el['src'] if logo_el and logo_el.has_attr('src') else None
                url = urljoin(base_url, link_el['href']) if link_el.has_attr('href') else None
                switched_from.append(ProductLink(name=name, logo_url=logo_url, url=url))
            reason_el = switched_section.select_one(REVIEW_REASON_SELECTOR) # Assumes reason is a <p> after links
            if reason_el: # Check if this p is not one of the product link wrappers
                 if not reason_el.find_parent('a', class_=REVIEW_PRODUCT_LINK_SELECTOR.split('.')[1]): # Ensure it's not part of a link
                    reason_for_switching = reason_el.get_text(strip=True)


        return CapterraReview(
            reviewer=reviewer_info, title=title, date_published_str=date_published_str, date_published=date_published_dt,
            overall_rating=overall_rating, rating_details=rating_details_list,
            overall_comment=overall_comment_text, pros=pros_text, cons=cons_text,
            source_type=source_type, source_tooltip=source_tooltip,
            alternatives_considered=alternatives_considered, reason_for_choosing=reason_for_choosing,
            switched_from=switched_from, reason_for_switching=reason_for_switching
        )
    except Exception as e:
        print(f"      [{thread_name}] Error parsing individual review card: {e}")
        # traceback.print_exc(file=sys.stdout) # DEBUG
        return None


def _scrape_capterra_reviews_for_url(
    product_url_str: str, company_slug: str, # company_slug is part of the URL for Capterra
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Optional[CapterraScrapeResult]:
    thread_name = f"Capterra-{company_slug[:15]}"
    scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for: {product_url_str}")
    
    driver = None
    try:
        driver = setup_selenium_driver()
        driver.get(product_url_str)
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        attempt_to_close_popups_capterra(driver, thread_name)

        # Extract initial product info
        product_name = driver.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR_HEADER).text.strip()
        product_logo_url = None
        try: product_logo_url = driver.find_element(By.CSS_SELECTOR, PRODUCT_LOGO_SELECTOR_HEADER).get_attribute('src')
        except NoSuchElementException: pass
        
        overall_rating_val, total_reviews_count_hdr = None, None
        try:
            rating_text_el = driver.find_element(By.CSS_SELECTOR, PRODUCT_RATING_TEXT_SELECTOR_HEADER)
            rating_text = rating_text_el.text.strip() # e.g., "4.7 (32)"
            match = re.match(r"([\d\.]+)\s*\((\d+)\)", rating_text)
            if match:
                overall_rating_val = float(match.group(1))
                total_reviews_count_hdr = int(match.group(2))
        except NoSuchElementException: pass
        except Exception as e_hdr_rating: print(f"    [{thread_name}] Minor error parsing header rating: {e_hdr_rating}")

        category_name = None
        try: category_name = driver.find_element(By.CSS_SELECTOR, PRODUCT_CATEGORY_BREADCRUMB_SELECTOR).text.strip()
        except NoSuchElementException: pass

        product_info = ProductInfo(
            name=product_name, capterra_url=HttpUrl(product_url_str), logo_url=product_logo_url,
            overall_product_rating=overall_rating_val, total_product_reviews_count_header=total_reviews_count_hdr,
            category=category_name
        )
        print(f"  [{thread_name}] Product: {product_info.name}, Category: {product_info.category}, Header Reviews: {product_info.total_product_reviews_count_header}")

        # Loop to click "Show more reviews"
        show_more_clicks = 0
        max_show_more_clicks = 200 # Safety break, adjust as needed (e.g. based on total_reviews_count_hdr if reliable)
        if total_reviews_count_hdr and total_reviews_count_hdr > 25: # Default 25 reviews per click
            max_show_more_clicks = (total_reviews_count_hdr // 25) + 5 # A bit of buffer

        while show_more_clicks < max_show_more_clicks :
            try:
                show_more_button = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S/2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR))
                )
                if not show_more_button.is_displayed() or not show_more_button.is_enabled():
                    print(f"  [{thread_name}] 'Show more' button found but not interactable. Assuming end of reviews.")
                    break
                
                num_reviews_before_click = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
                
                print(f"  [{thread_name}] Clicking 'Show more reviews' (Attempt {show_more_clicks + 1}). Reviews so far (approx): {num_reviews_before_click}")
                if not try_click(driver, show_more_button):
                    print(f"  [{thread_name}] Failed to click 'Show more' button. Assuming end or overlay.")
                    attempt_to_close_popups_capterra(driver, thread_name) # Try closing popups and one more attempt
                    try: # Re-find button
                        show_more_button = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S/3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR))
                        )
                        if not show_more_button.is_displayed() or not try_click(driver, show_more_button):
                             print(f"  [{thread_name}] Final click attempt failed. Stopping 'Show more'.")
                             break
                    except TimeoutException:
                        print(f"  [{thread_name}] 'Show more' button not found after popup clear. Assuming end.")
                        break


                show_more_clicks += 1
                time.sleep(AFTER_SHOW_MORE_CLICK_SLEEP_S) # Wait for content to load

                # Optional: Check if number of reviews increased to break early if button persists but no new content
                # num_reviews_after_click = len(driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}"))
                # if num_reviews_after_click == num_reviews_before_click and show_more_clicks > 2: # check after a couple of clicks
                #     print(f"  [{thread_name}] Number of reviews did not increase after click. Assuming end.")
                #     break

            except TimeoutException:
                print(f"  [{thread_name}] 'Show more reviews' button not found. Assuming all reviews are loaded.")
                break
            except Exception as e_sm:
                print(f"  [{thread_name}] Error during 'Show more' click loop: {e_sm}")
                break
        
        print(f"  [{thread_name}] Finished clicking 'Show more'. Total clicks: {show_more_clicks}. Proceeding to parse.")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER)

        all_reviews_on_page: List[CapterraReview] = []
        review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)
        if review_cards_container:
            review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR)
            print(f"  [{thread_name}] Found {len(review_card_soups)} review card elements for parsing.")
            
            for idx, card_soup in enumerate(review_card_soups):
                # print(f"    [{thread_name}] Parsing review card {idx+1}/{len(review_card_soups)}") # Verbose
                review = _parse_individual_review_card(card_soup, product_url_str, thread_name)
                if review:
                    # Apply date filters if provided
                    if start_date_filter and review.date_published and review.date_published < start_date_filter:
                        continue
                    if end_date_filter and review.date_published and review.date_published > end_date_filter:
                        continue
                    all_reviews_on_page.append(review)
        else:
            print(f"  [{thread_name}] No review cards container found ('{REVIEW_CARDS_CONTAINER_SELECTOR}').")

        scrape_duration = time.perf_counter() - scrape_start_time
        print(f"  [{thread_name}] Finished scraping {product_url_str} in {scrape_duration:.2f}s. Parsed {len(all_reviews_on_page)} reviews.")
        
        return CapterraScrapeResult(
            product_info=product_info,
            reviews=all_reviews_on_page,
            reviews_count_scraped=len(all_reviews_on_page),
            scrape_duration_seconds=scrape_duration
        )

    except Exception as e_main_scrape:
        print(f"  [{thread_name}] MAJOR ERROR scraping {product_url_str}: {e_main_scrape}")
        traceback.print_exc()
        return None
    finally:
        if driver:
            driver.quit()

# --- Main Orchestrator for Capterra ---
def scrape_capterra_sync(
    product_url_str: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    
    parsed_url = urlparse(product_url_str)
    path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
    company_slug = "unknown-product"
    if len(path_segments) >= 3 and path_segments[0] == "p": # e.g. /p/ID/SLUG/reviews
        company_slug = path_segments[2] 

    print(f"Orchestrating Capterra scrape for: {product_url_str} (Slug: {company_slug})")
    
    result_obj = _scrape_capterra_reviews_for_url(product_url_str, company_slug, start_date_filter, end_date_filter)

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
            "summary": {"product_url": product_url_str}
        }

# --- FastAPI Endpoint ---
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

    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided.")

    results: Dict[str, Dict[str, Any]] = {}
    valid_scrape_tasks = []

    for url_obj in request.urls:
        url_str = str(url_obj)
        if "capterra.com/p/" not in url_str or "/reviews" not in url_str:
             results[url_str] = {"status": "error", "message": f"Invalid Capterra URL format: {url_str}. Expected format like 'https://www.capterra.com/p/PRODUCT_ID/PRODUCT_SLUG/reviews/'"}
             continue
        valid_scrape_tasks.append(
            asyncio.to_thread(scrape_capterra_sync, url_str, start_date_filter, end_date_filter)
        )

    scraped_results_or_exceptions = await asyncio.gather(*valid_scrape_tasks, return_exceptions=True) if valid_scrape_tasks else []
    
    task_idx = 0
    for url_obj in request.urls:
        original_url_str = str(url_obj)
        if original_url_str in results: # Already processed as invalid
            continue
        
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} (Capterra v{app.version}) EXCEPTION: {result_or_exc}")
                # traceback.print_exc(file=sys.stdout)
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(result_or_exc).__name__}. Check server logs."}
            elif isinstance(result_or_exc, dict):
                results[original_url_str] = result_or_exc
            else:
                results[original_url_str] = {"status": "error", "message": "Unexpected internal result type."}
            task_idx += 1
        else: # Should not happen if logic is correct
            results[original_url_str] = {"status": "error", "message": "Scraping task result missing."}


    print(f"Finished Capterra API request processing (v{app.version}).")
    return results


# For local testing:
# if __name__ == "__main__":
#     async def main_test_capterra():
#         test_url = "https://www.capterra.com/p/253176/Google-One/reviews/"
#         # test_url_2 = "https://www.capterra.com/p/132045/Slack/reviews/"
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url)]) #, HttpUrl(test_url_2)])
#         results = await scrape_capterra_endpoint(test_request)
#         print(json.dumps(results, indent=2, default=str)) # Use default=str for datetime objects
#     asyncio.run(main_test_capterra())
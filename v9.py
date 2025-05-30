import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse
import traceback
import sys
import warnings

# Suppress specific UserWarnings from selenium-driverless
warnings.filterwarnings(
    "ignore",
    message="got execution_context_id and unique_context=True, defaulting to execution_context_id",
    category=UserWarning,
    module="selenium_driverless.types.deserialize"
)
warnings.filterwarnings(
    "ignore",
    message="got execution_context_id but no target_id, defaulting to main target",
    category=UserWarning,
    module="selenium_driverless.types.deserialize"
)

# --- Pydantic Models (from v5.py) ---
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
    title="Capterra Scraper API - Driverless (Stable Setup Attempt)",
    description="Selenium-Driverless focusing on stable initialization and reliable clicks.",
    version="1.4.0" # Incremented
)

from bs4 import BeautifulSoup
import selenium_driverless.webdriver as driverless_webdriver
from selenium_driverless.types.by import By
from selenium.common.exceptions import ( 
    TimeoutException, NoSuchElementException, ElementClickInterceptedException,
    StaleElementReferenceException, ElementNotInteractableException
)

ua = None 

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants for Capterra ---
SELENIUM_PAGE_TIMEOUT_S = 40 # Standard timeout for page load
SELENIUM_ELEMENT_FIND_TIMEOUT_S = 10 
SELENIUM_POPUP_FIND_TIMEOUT_S = 5 
SELENIUM_INTERACTION_TIMEOUT_S = 7 
IFRAME_CONTENT_DOC_TIMEOUT_S = 6.0

INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(3.5, 5.0) # Slightly more generous initial sleep
LOADING_SPINNER_SELECTOR = 'svg[class*="s1xr3lbz"]'
AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S = 20 

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10'
OVERLAY_POPUP_CLOSE_BUTTON_SELECTOR = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"].icon-font-x'

# BS Selectors (from v5.py)
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


async def prepare_driverless_driver() -> driverless_webdriver.Chrome:
    """Setup selenium-driverless Chrome driver."""
    options = driverless_webdriver.ChromeOptions()
    # Basic, reliable options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--log-level=3') # Suppress non-critical browser logs
    options.add_argument("--start-maximized") 
    options.add_argument("--lang=en-US,en;q=0.9")
    # selenium-driverless aims to handle stealth automatically. 
    # Avoid adding too many experimental options that might conflict.
    # options.add_argument("--disable-blink-features=AutomationControlled") # This is often default in driverless
    # options.add_experimental_option("excludeSwitches", ["enable-automation"]) # Usually handled by driverless
    # options.add_experimental_option('useAutomationExtension', False) # Usually handled by driverless

    try:
        print(f"    [{asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'}] Initializing Chrome with options...")
        driver = await driverless_webdriver.Chrome(options=options)
        print(f"    [{asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'}] Chrome initialized. Applying basic stealth script.")
        
        # Apply minimal navigator.webdriver override, as driverless often handles this
        # but explicit can sometimes help certain sites.
        await driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })
        print(f"    [{asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'}] Stealth script applied.")
        return driver
    except Exception as e:
        task_name = asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'
        print(f"  [{task_name}] CRITICAL DRIVER SETUP ERROR: {type(e).__name__} - {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup selenium-driverless driver: {e}")


async def async_is_element_displayed_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element: return False
    try:
        return await driver.execute_script(
            """const elem = arguments[0]; if (!elem || !elem.getClientRects || !elem.getClientRects().length) return false; 
            const style = window.getComputedStyle(elem); if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) < 0.1) return false;
            return true;""", element )
    except Exception: return False

async def async_is_element_present_in_dom_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element: return False
    try: return await driver.execute_script("return arguments[0].isConnected;", element)
    except Exception: return False


async def try_click_element(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement, thread_name: str = "DefaultThread", always_js_click: bool = False) -> bool:
    action_description = "provided_element"
    try:
        if not element: return False
        if not await async_is_element_present_in_dom_js(driver, element): return False
        
        try: 
            await driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", element)
            await asyncio.sleep(0.1)
        except Exception: pass
        
        if always_js_click:
            await driver.execute_script("arguments[0].click();", element)
            return True

        try:
            await element.click(move_to=True) 
            return True
        except (ElementClickInterceptedException, ElementNotInteractableException, StaleElementReferenceException, IndexError) as e_click_direct:
            pass 
        except Exception as e_other_direct:
            print(f"    [{thread_name}][try_click_element] Unexpected error during direct click: {type(e_other_direct).__name__}. Falling back to JS click.")
            pass

        await asyncio.sleep(0.05) 
        if not await async_is_element_present_in_dom_js(driver, element): return False
        await driver.execute_script("arguments[0].click();", element)
        return True
    except Exception as e_overall: 
        print(f"    [{thread_name}][try_click_element] Overall unexpected error: {type(e_overall).__name__} - {e_overall}")
        return False

async def handle_specific_overlay_popup(driver: driverless_webdriver.Chrome, thread_name: str) -> bool:
    try:
        # print(f"      [{thread_name}] Checking for specific overlay popup...")
        close_buttons = await driver.find_elements(By.CSS_SELECTOR, OVERLAY_POPUP_CLOSE_BUTTON_SELECTOR, timeout=SELENIUM_POPUP_FIND_TIMEOUT_S / 2)
        if close_buttons:
            for btn in close_buttons:
                if await async_is_element_displayed_js(driver, btn):
                    if await try_click_element(driver, btn, thread_name=f"{thread_name}-OverlayPopup", always_js_click=True):
                        print(f"      [{thread_name}] Clicked specific overlay popup close button.")
                        await asyncio.sleep(0.7)
                        return True
    except TimeoutException: pass
    except Exception as e: print(f"      [{thread_name}] Error handling specific overlay popup: {type(e).__name__} - {e}")
    return False

async def async_wait_until_reviews_load(driver: driverless_webdriver.Chrome, initial_review_count: int, review_card_selector: str, loader_selector: str, timeout: float, thread_name: str ) -> bool:
    start_time = time.monotonic()
    loader_was_seen_and_disappeared_without_increase = False

    while time.monotonic() - start_time < timeout:
        current_reviews_elements = []
        try:
            current_reviews_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
            if len(current_reviews_elements) > initial_review_count: return True
        except TimeoutException: pass 
        except Exception as e_find: print(f"      [{thread_name}] Error finding reviews in wait: {type(e_find).__name__}"); await asyncio.sleep(0.2); continue

        try: # Check loader status
            loader_elements = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=0.25)
            is_loader_currently_displayed = any(await async_is_element_displayed_js(driver, el) for el in loader_elements)
            
            if loader_was_seen_and_disappeared_without_increase and not is_loader_currently_displayed:
                # If loader was seen, then disappeared, and review count hasn't increased, assume loading for this click is done.
                return False # Signal that this wait cycle should end
            if is_loader_currently_displayed:
                loader_was_seen_and_disappeared_without_increase = True # Mark that we've seen it
            elif loader_was_seen_and_disappeared_without_increase and not is_loader_currently_displayed: # Loader gone for good this cycle
                 loader_was_seen_and_disappeared_without_increase = True # Keep true to ensure we check review count one last time

        except Exception: pass
        await asyncio.sleep(0.35)
        
    final_review_elements = []
    try: final_review_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
    except Exception: pass
    return len(final_review_elements) > initial_review_count


async def _load_all_capterra_reviews_driverless(product_url_str: str, company_slug: str) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoadV3-{company_slug[:10]}"; print(f"  [{thread_name}] Starting Driverless v3 for: {product_url_str}")
    product_name_guess = company_slug.replace("-"," ").title()
    page_source_result: Optional[str] = None
    
    driver: Optional[driverless_webdriver.Chrome] = None # Ensure driver is defined for finally block
    try:
        driver = await prepare_driverless_driver() # Use new setup function
        if not driver: # Should have raised in prepare_driverless_driver if failed
            print(f"  [{thread_name}] Driver initialization failed in prepare_driverless_driver."); return None, product_name_guess

        print(f"    [{thread_name}] Navigating to URL: {product_url_str}")
        await driver.get(product_url_str, timeout=SELENIUM_PAGE_TIMEOUT_S)
        print(f"    [{thread_name}] Navigation complete. Initial sleep: {INITIAL_PAGE_LOAD_SLEEP_S:.2f}s")
        await asyncio.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        
        await handle_specific_overlay_popup(driver, thread_name)

        show_more_clicks = 0
        try: 
            await driver.find_element(By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
        except TimeoutException: 
            print(f"  [{thread_name}] Review container not found. Getting page source."); 
            page_source_result = await driver.page_source; 
            return page_source_result, product_name_guess
        
        max_show_more_attempts = 250 
        for attempt_loop in range(max_show_more_attempts):
            initial_review_elements = await driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
            initial_review_count_dom = len(initial_review_elements)
            # print(f"    [{thread_name}] Loop iter {attempt_loop+1}. Current reviews: {initial_review_count_dom}")
            try:
                show_more_button_el = await driver.find_element(By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S) # Give more time to find this button
                
                clicked = await try_click_element(driver, element=show_more_button_el, thread_name=thread_name, always_js_click=True)
                
                if not clicked:
                    print(f"    [{thread_name}] try_click (forced JS) failed for 'Show More' on attempt {attempt_loop+1}. Assuming end."); 
                    break 
                
                show_more_clicks += 1; 
                # print(f"      [{thread_name}] Click #{show_more_clicks} performed. Waiting for content...")
                
                reviews_loaded_successfully = await async_wait_until_reviews_load(
                    driver, initial_review_count_dom, 
                    f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", 
                    LOADING_SPINNER_SELECTOR, 
                    AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S, 
                    thread_name
                )
                
                if not reviews_loaded_successfully:
                    print(f"      [{thread_name}] Wait condition indicated no new reviews loaded after click #{show_more_clicks}. Assuming end.")
                    break 
                
                # Check for overlay again periodically
                if show_more_clicks > 0 and show_more_clicks % 5 == 0 : # Check every 5 clicks
                     await handle_specific_overlay_popup(driver, thread_name)

            except TimeoutException: print(f"  [{thread_name}] 'Show more' button not found (iter {attempt_loop+1}). Assuming all loaded."); break
            except Exception as e_sm_loop: print(f"  [{thread_name}] Error in 'Show more' loop (iter {attempt_loop+1}): {type(e_sm_loop).__name__} - {e_sm_loop}"); break 
        
        print(f"  [{thread_name}] 'Show more' loop finished. Clicks: {show_more_clicks}. Retrieving page source."); 
        page_source_result = await driver.page_source
        
    except RuntimeError as e_setup: print(f"  [{thread_name}] CRITICAL DRIVER SETUP ERROR: {e_setup}"); traceback.print_exc()
    except Exception as e_load_main: print(f"  [{thread_name}] MAJOR ERROR during Driverless loading: {type(e_load_main).__name__} - {e_load_main}"); traceback.print_exc()
    finally:
        if driver:
            print(f"  [{thread_name}] Attempting to quit driver.")
            try:
                await driver.quit()
                print(f"  [{thread_name}] Driver quit successfully.")
            except Exception as e_quit:
                print(f"  [{thread_name}] Error during driver.quit(): {type(e_quit).__name__} - {e_quit}")
    return page_source_result, product_name_guess

# --- Parsing functions (from v5.py) ---
def parse_capterra_datetime_for_output(date_str: str) -> Tuple[Optional[str], Optional[datetime]]:
    if not date_str: return None, None
    parsed_dt = None; formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try: parsed_dt = datetime.strptime(date_str.strip(), fmt); break
        except ValueError: continue
    if parsed_dt: output_str = parsed_dt.strftime("%Y-%m-%d") + " 00:00:00 +0000"; return output_str, parsed_dt 
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
        date_str_on_page = "Unknown Date"; date_el_found = None
        title_block_parent_scope = review_card_soup.select_one(f'{BS_REVIEW_TITLE_SELECTOR}')
        if title_block_parent_scope: title_block_parent_scope = title_block_parent_scope.parent
        if title_block_parent_scope:
            date_candidates = title_block_parent_scope.find_all('div', class_='typo-0 text-neutral-90', recursive=False)
            for cand in date_candidates:
                 if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', cand.get_text(strip=True), re.IGNORECASE): date_el_found = cand; break
            if not date_el_found:
                current_el = title_block_parent_scope
                for _ in range(2): 
                    if not current_el: break
                    for sib in current_el.find_next_siblings('div', class_='typo-0 text-neutral-90', limit=2):
                        if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', sib.get_text(strip=True), re.IGNORECASE): date_el_found = sib; break
                    if date_el_found: break; current_el = current_el.parent
        if date_el_found: date_str_on_page = date_el_found.get_text(strip=True)
        datetime_str_output, _ = parse_capterra_datetime_for_output(date_str_on_page)
        rating_el = review_card_soup.select_one(BS_REVIEW_CARD_OVERALL_RATING_SELECTOR)
        rating_str = rating_el.get_text(strip=True) if rating_el else "0.0"
        pros_el = review_card_soup.select_one(BS_REVIEW_PROS_SELECTOR)
        pros = pros_el.get_text(strip=True) if pros_el else None
        cons_el = review_card_soup.select_one(BS_REVIEW_CONS_SELECTOR)
        cons = cons_el.get_text(strip=True) if cons_el else None; review_text = ""
        main_content_block = review_card_soup.select_one('div[class*="!mt-4 space-y-6"]')
        if main_content_block:
            p_tags = main_content_block.find_all('p', recursive=False); candidate_texts = []
            for p_tag in p_tags:
                p_text_content = p_tag.get_text(strip=True)
                is_pros_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Pros", re.I))))
                is_cons_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Cons", re.I))))
                if p_text_content and p_text_content != pros and p_text_content != cons and not is_pros_p and not is_cons_p: candidate_texts.append(p_text_content)
            if candidate_texts: review_text = " ".join(candidate_texts)
        return CapterraIndividualReview(title=title, text=review_text, reviewer=reviewer_name, time_used_product=time_used, reviewer_avatar=reviewer_avatar_url, datetime=datetime_str_output, rating=rating_str, pros=pros, cons=cons)
    except Exception: return None

def _parse_capterra_html_for_reviews(page_source: str, original_url_str: str, selenium_product_name_guess: str, thread_name: str) -> CapterraScrapeResultOutput:
    soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER); parsed_reviews_list: List[CapterraIndividualReview] = []
    product_name_scraped = selenium_product_name_guess; product_category_scraped = None 
    h1_title_el = soup.select_one(BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR) 
    if h1_title_el and "reviews of" in h1_title_el.text.lower(): product_name_scraped = h1_title_el.text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
    else: name_header_el = soup.select_one(BS_PRODUCT_NAME_HEADER_SELECTOR); product_name_scraped = name_header_el.get_text(strip=True) if name_header_el else product_name_scraped
    cat_el = soup.select_one(BS_PRODUCT_CATEGORY_BREADCRUMB_SELECTOR); product_category_scraped = cat_el.get_text(strip=True) if cat_el else None
    overall_rating_str, ease_of_use_str, customer_service_str, features_str, value_money_str = None, None, None, None, None; review_count_from_display = None
    overall_product_rating_el = soup.select_one(BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR)
    if overall_product_rating_el: match = re.match(r"([\d\.]+)(?:\s*\((\d+)\))?", overall_product_rating_el.get_text(strip=True)); overall_rating_str, review_count_from_display = (match.group(1), int(match.group(2))) if match and match.group(2) else (match.group(1) if match else None, None)
    if not review_count_from_display:
        review_count_display_el = soup.select_one(BS_REVIEW_COUNT_DISPLAY_SELECTOR)
        if review_count_display_el: text = review_count_display_el.get_text(strip=True); count_match_of = re.search(r"of\s+(\d+)\s+Reviews", text, re.IGNORECASE); count_match_showing_only = re.search(r"Showing\s+(\d+)\s+Reviews", text, re.IGNORECASE); review_count_from_display = int(count_match_of.group(1)) if count_match_of else (int(count_match_showing_only.group(1)) if count_match_showing_only else None)
    summary_section = soup.select_one(BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR)
    if summary_section:
        def get_rating_from_summary(selector): el = summary_section.select_one(selector); return el.get_text(strip=True).split()[0] if el and el.get_text(strip=True) else None
        ease_of_use_str = get_rating_from_summary(BS_EASE_OF_USE_TOTAL_RATING_SELECTOR); customer_service_str = get_rating_from_summary(BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR); features_str = get_rating_from_summary(BS_FEATURES_TOTAL_RATING_SELECTOR); value_money_str = get_rating_from_summary(BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR)
    totals = CapterraReviewTotals(review_count=review_count_from_display, overall_rating=overall_rating_str, ease_of_use_rating=ease_of_use_str, customer_service_rating=customer_service_str, functionality_rating=features_str, value_for_money_rating=value_money_str)
    review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)
    if review_cards_container: review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR); parsed_reviews_list.extend(review for card_soup in review_card_soups if (review := _parse_individual_review_card_revised(card_soup, thread_name)))
    else: print(f"  [{thread_name}] Review cards container not found for BS4 parsing.")
    return CapterraScrapeResultOutput(totals=totals, reviews=parsed_reviews_list, product_name_scraped=product_name_scraped, product_category_scraped=product_category_scraped, original_url=HttpUrl(original_url_str), reviews_count_scraped=len(parsed_reviews_list), scrape_duration_seconds=0)


# --- FastAPI endpoint and orchestration ---
async def scrape_capterra_async( product_url_str: str, start_date_filter_dt: Optional[datetime.date] = None, end_date_filter_dt: Optional[datetime.date] = None) -> Dict[str, Any]:
    overall_start_time = time.perf_counter(); parsed_url = urlparse(product_url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]; company_slug = path_segments[2] if len(path_segments) >= 3 and path_segments[0].lower() == "p" else "unknown-slug"
    page_source, product_name_selenium_guess = await _load_all_capterra_reviews_driverless(product_url_str, company_slug)
    if not page_source: return {"status": "error", "message": "Failed to load page content with Async Driverless.", "data": None, "summary": {"product_url": product_url_str, "product_name_guess": product_name_selenium_guess}}
    thread_name_parse = f"CapterraParse-{company_slug[:10]}"
    parsed_data_obj = _parse_capterra_html_for_reviews(page_source, product_url_str, product_name_selenium_guess, thread_name_parse); parsed_data_obj.scrape_duration_seconds = round(time.perf_counter() - overall_start_time, 2)
    return {"status": "success" if parsed_data_obj.reviews_count_scraped > 0 else "no_reviews_found", 
            "data": parsed_data_obj.model_dump(mode='json', by_alias=True), 
            "summary": {"product_name": parsed_data_obj.product_name_scraped, 
                        "total_reviews_scraped": parsed_data_obj.reviews_count_scraped, 
                        "duration_seconds": parsed_data_obj.scrape_duration_seconds}}

@app.post("/scrape-capterra", tags=["Capterra"])
async def scrape_capterra_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    start_date_filter_dt, end_date_filter_dt = None, None 
    if request.start_date_str:
        try: start_date_filter_dt = datetime.strptime(request.start_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str format (YYYY-MM-DD).")
    if request.end_date_str:
        try: end_date_filter_dt = datetime.strptime(request.end_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str format (YYYY-MM-DD).")
    if start_date_filter_dt and end_date_filter_dt and start_date_filter_dt > end_date_filter_dt: raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
    if not request.urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    
    results: Dict[str, Dict[str, Any]] = {}; valid_urls_for_processing = []; url_to_task_map = {} 
    for url_obj in request.urls:
        s_url = str(url_obj)
        if "capterra.com/p/" in s_url and "/reviews" in s_url: valid_urls_for_processing.append(url_obj)
        else: results[s_url] = {"status": "error", "message": f"Invalid Capterra URL format: {s_url} or skipped."}
    if not valid_urls_for_processing:
        if results: return results 
        raise HTTPException(status_code=400, detail="No valid Capterra URLs provided.")
    
    tasks = [asyncio.create_task(scrape_capterra_async(str(url), None, None), name=f"scrape_{str(url)}") for url in valid_urls_for_processing]
    for task, url_obj in zip(tasks, valid_urls_for_processing): url_to_task_map[task] = str(url_obj)
    
    task_execution_results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res_or_exc in enumerate(task_execution_results):
        original_url = url_to_task_map[tasks[i]] 
        if isinstance(res_or_exc, Exception):
            print(f"Task for {original_url} (v{app.version}) EXCEPTION: {type(res_or_exc).__name__} - {res_or_exc}"); traceback.print_exc(file=sys.stdout)
            results[original_url] = {"status": "error", "message": f"Scraping task failed: {type(res_or_exc).__name__}."}
        else: results[original_url] = res_or_exc 
    return results
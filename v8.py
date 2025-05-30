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
    title="Capterra Scraper API - Async Driverless Loader (Focused Clicker v3)",
    description="Selenium-Driverless (async) for clicking 'Show More'. Focused on reliable clicks and completion.",
    version="1.3.8" # Incremented
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
SELENIUM_PAGE_TIMEOUT_S = 35
SELENIUM_ELEMENT_FIND_TIMEOUT_S = 8 
SELENIUM_INTERACTION_TIMEOUT_S = 5 # Timeout for element to be found by try_click if locator used

INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(2.5, 3.5) 
LOADING_SPINNER_SELECTOR = 'svg[class*="s1xr3lbz"]'
AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S = 18 # Increased slightly for more patience

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10'

# BS Selectors (unchanged)
BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR = 'h1[data-testid="richcontent-title"]'
BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR = 'div.cnfb6xn'
BS_REVIEWER_NAME_SELECTOR = 'span[class*="reviewer-name"]'
BS_REVIEWER_AVATAR_IMG_SELECTOR = 'img[class*="reviewer-avatar"]'
BS_REVIEWER_INITIALS_FALLBACK_SELECTOR = 'div[class*="reviewer-initials"]'
BS_REVIEWER_INFO_CONTAINER_SELECTOR = 'div[class*="reviewer-info"]'
BS_REVIEW_TITLE_SELECTOR = 'h3[class*="review-title"]'
BS_REVIEW_CARD_OVERALL_RATING_SELECTOR = 'div[class*="rating-value"]'
BS_REVIEW_PROS_SELECTOR = 'div[class*="pros"] p'
BS_REVIEW_CONS_SELECTOR = 'div[class*="cons"] p'
BS_PRODUCT_NAME_HEADER_SELECTOR = 'h1[class*="product-name"]'
BS_PRODUCT_CATEGORY_BREADCRUMB_SELECTOR = 'a[class*="breadcrumb-category"]'
BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR = 'div[class*="overall-rating-header"]'
BS_REVIEW_COUNT_DISPLAY_SELECTOR = 'div[class*="review-count-display"]'
BS_EASE_OF_USE_TOTAL_RATING_SELECTOR = 'div[class*="ease-of-use-rating"]'
BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR = 'div[class*="customer-service-rating"]'
BS_FEATURES_TOTAL_RATING_SELECTOR = 'div[class*="features-rating"]'
BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR = 'div[class*="value-for-money-rating"]'

def prepare_driverless_options() -> driverless_webdriver.ChromeOptions:
    options = driverless_webdriver.ChromeOptions()
    # options.headless = True 
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--log-level=3'); options.add_argument("--start-maximized")
    options.add_argument("--lang=en-US,en;q=0.9")
    return options

async def async_is_element_displayed_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element: return False
    try:
        return await driver.execute_script(
            """const elem = arguments[0]; if (!elem || !elem.getClientRects || !elem.getClientRects().length) return false; 
            const style = window.getComputedStyle(elem); if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
            return true;""", element )
    except Exception: return False

async def async_is_element_present_in_dom_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element: return False
    try:
        return await driver.execute_script("return arguments[0].isConnected;", element)
    except Exception: return False


async def try_click(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement, thread_name: str = "DefaultThread", force_js_click: bool = False) -> bool:
    action_description = "provided_element"
    try:
        if not element: return False
        if not await async_is_element_present_in_dom_js(driver, element): return False # Check if stale before any action
        
        try: 
            await driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", element)
            await asyncio.sleep(0.1) # Minimal pause after scroll
        except Exception: pass
        
        if force_js_click:
            # print(f"    [{thread_name}][try_click] Forcing JS click for {action_description}.")
            await driver.execute_script("arguments[0].click();", element)
            return True

        # Attempt direct click first (unless forced_js)
        try:
            await element.click(move_to=True) 
            return True
        except (ElementClickInterceptedException, ElementNotInteractableException, StaleElementReferenceException, IndexError) as e_click_direct:
            print(f"    [{thread_name}][try_click] Direct click failed ({type(e_click_direct).__name__}). Falling back to JS click for {action_description}.")
            # Fall through to JS click
            pass
        except Exception as e_other_direct: # Catch any other unexpected error during direct click
            print(f"    [{thread_name}][try_click] Unexpected error during direct click for {action_description}: {type(e_other_direct).__name__}. Falling back to JS click.")
            # Fall through to JS click
            pass

        # Fallback to JavaScript click
        # print(f"    [{thread_name}][try_click] Attempting JS click fallback for {action_description}.")
        await asyncio.sleep(0.05) 
        if not await async_is_element_present_in_dom_js(driver, element): return False # Re-check DOM
        await driver.execute_script("arguments[0].click();", element)
        return True

    except Exception as e_overall: 
        print(f"    [{thread_name}][try_click] Overall unexpected error in try_click for {action_description}: {type(e_overall).__name__} - {e_overall}")
        return False

async def async_wait_until_reviews_load_or_loader_gone(driver: driverless_webdriver.Chrome, initial_review_count: int, review_card_selector: str, loader_selector: str, timeout: float, thread_name: str ) -> bool:
    # print(f"      [{thread_name}] Waiting for reviews. Initial: {initial_review_count}. Timeout: {timeout}s")
    loader_initially_present = False
    try:
        loader_elements = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=0.5)
        for el in loader_elements:
            if await async_is_element_displayed_js(driver, el): loader_initially_present = True; break
    except Exception: pass 
    
    start_time = time.monotonic()
    last_check_review_count = initial_review_count

    while time.monotonic() - start_time < timeout:
        current_reviews_elements = []
        try:
            current_reviews_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
            last_check_review_count = len(current_reviews_elements)
            if last_check_review_count > initial_review_count:
                # print(f"      [{thread_name}] Review count increased to {last_check_review_count}. Success.")
                return True # Primary success condition
        except TimeoutException: # It's okay if finding reviews times out, page might be loading
            # print(f"      [{thread_name}] Timeout finding review cards, will retry.")
            pass 
        
        if loader_initially_present: # Only actively check for loader disappearance if it was seen
            try:
                any_loader_displayed = False
                loader_elements_check = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=0.25) # Very quick check
                for el_check in loader_elements_check:
                    if await async_is_element_displayed_js(driver, el_check): any_loader_displayed = True; break
                
                if not any_loader_displayed: # Loader was present, now it's gone
                    # print(f"      [{thread_name}] Loader disappeared. Final review count check.")
                    await asyncio.sleep(0.25) # Give DOM a bit more time to settle
                    final_reviews_after_loader = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
                    if len(final_reviews_after_loader) > initial_review_count:
                        # print(f"      [{thread_name}] Reviews increased after loader gone ({len(final_reviews_after_loader)}). Success.")
                        return True
                    # print(f"      [{thread_name}] Loader gone, but review count not increased from {initial_review_count}. Considering this as 'load attempt complete'.")
                    return False # Loader gone, no new reviews = likely end of load for this click
            except Exception: pass # Ignore errors in this sub-check, main loop continues
        
        await asyncio.sleep(0.3) # Polling interval; slightly longer if loader wasn't initially seen
        
    # Timeout reached
    # print(f"      [{thread_name}] Wait loop timed out. Initial: {initial_review_count}, Last seen: {last_check_review_count}")
    return last_check_review_count > initial_review_count # Final check if any reviews loaded just before timeout


async def _load_all_capterra_reviews_selenium(product_url_str: str, company_slug: str) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoad-{company_slug[:12]}"; print(f"  [{thread_name}] Starting Focused Clicker v3 for: {product_url_str}")
    product_name_guess = company_slug.replace("-"," ").title()
    page_source_result: Optional[str] = None
    options = prepare_driverless_options()
    
    try:
        async with driverless_webdriver.Chrome(options=options) as driver: # Reverted to async with
            await driver.get(product_url_str, timeout=SELENIUM_PAGE_TIMEOUT_S)
            await asyncio.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
            
            show_more_clicks = 0
            try: 
                await driver.find_element(By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
            except TimeoutException: 
                print(f"  [{thread_name}] Review container not found initially. Getting page source as is."); 
                page_source_result = await driver.page_source; 
                return page_source_result, product_name_guess # Return the fallback guess
            
            max_show_more_attempts = 200 
            for attempt_loop in range(max_show_more_attempts):
                initial_review_elements = await driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
                initial_review_count_dom = len(initial_review_elements)
                # print(f"    [{thread_name}] Loop iter {attempt_loop+1}. Reviews: {initial_review_count_dom}")
                try:
                    show_more_button_el = await driver.find_element(By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
                    
                    # Force JS click for the "Show More" button due to its instability with direct clicks
                    clicked = await try_click(driver, element=show_more_button_el, thread_name=thread_name, force_js_click=True)
                    
                    if not clicked:
                        print(f"    [{thread_name}] try_click (forced JS) failed for 'Show More' on attempt {attempt_loop+1}. Assuming end or button issue."); 
                        break 
                    
                    show_more_clicks += 1; 
                    # print(f"      [{thread_name}] Click #{show_more_clicks} (JS) performed. Waiting for content...")
                    
                    # Wait for reviews to load or spinner to disappear
                    wait_success = await async_wait_until_reviews_load_or_loader_gone(
                        driver, initial_review_count_dom, 
                        f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", 
                        LOADING_SPINNER_SELECTOR, 
                        AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S, 
                        thread_name
                    )
                    
                    if not wait_success:
                        print(f"      [{thread_name}] Wait condition returned false after click #{show_more_clicks} (no new reviews or loader gone without increase). Assuming end.")
                        # Check one last time if review count increased despite wait_success being false
                        final_check_elements = await driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", timeout=1)
                        if len(final_check_elements) > initial_review_count_dom:
                             print(f"      [{thread_name}] Confirmed review increase ({len(final_check_elements)}) despite wait_success=false. Continuing.")
                        else:
                            break # Break if wait_success is false and count truly didn't change
                
                except TimeoutException: print(f"  [{thread_name}] 'Show more' button not found (iter {attempt_loop+1}). Assuming all loaded."); break
                except Exception as e_sm_loop: print(f"  [{thread_name}] Error in 'Show more' loop (iter {attempt_loop+1}): {type(e_sm_loop).__name__} - {e_sm_loop}"); break 
            
            print(f"  [{thread_name}] 'Show more' loop finished. Clicks: {show_more_clicks}. Retrieving page source."); 
            page_source_result = await driver.page_source
        
    except RuntimeError as e_setup: print(f"  [{thread_name}] CRITICAL DRIVER SETUP ERROR for {product_url_str}: {e_setup}"); traceback.print_exc()
    except Exception as e_load_main: print(f"  [{thread_name}] MAJOR ERROR during Async Driverless loading for {product_url_str}: {type(e_load_main).__name__} - {e_load_main}"); traceback.print_exc()
    # `async with` handles driver.quit() implicitly and more reliably
    return page_source_result, product_name_guess


# --- Parsing functions and FastAPI endpoint (remain unchanged from v1.3.7) ---
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
        reviewer_name_el = review_card_soup.select_one(BS_REVIEWER_NAME_SELECTOR); avatar_el = review_card_soup.select_one(BS_REVIEWER_AVATAR_IMG_SELECTOR)
        if reviewer_name_el: reviewer_name = reviewer_name_el.get_text(strip=True)
        if avatar_el and avatar_el.has_attr('src'): reviewer_avatar_url = avatar_el['src']
        if not reviewer_name: initials_el = review_card_soup.select_one(BS_REVIEWER_INITIALS_FALLBACK_SELECTOR); reviewer_name = initials_el.get_text(strip=True) if initials_el else None
        details_container = review_card_soup.select_one(BS_REVIEWER_INFO_CONTAINER_SELECTOR)
        if details_container: all_details_text = details_container.get_text(separator="\n", strip=True); time_used_match = re.search(r"Used the software for:\s*(.+)", all_details_text, re.IGNORECASE); time_used = time_used_match.group(1).strip().rstrip('.') if time_used_match else None
        title_el = review_card_soup.select_one(BS_REVIEW_TITLE_SELECTOR); title = title_el.get_text(strip=True) if title_el else "No Title"
        date_str_on_page = "Unknown Date"; date_el_found = None; title_block_parent_scope = review_card_soup.select_one(f'{BS_REVIEW_TITLE_SELECTOR}')
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
        rating_el = review_card_soup.select_one(BS_REVIEW_CARD_OVERALL_RATING_SELECTOR); rating_str = rating_el.get_text(strip=True) if rating_el else "0.0"
        pros_el = review_card_soup.select_one(BS_REVIEW_PROS_SELECTOR); pros = pros_el.get_text(strip=True) if pros_el else None
        cons_el = review_card_soup.select_one(BS_REVIEW_CONS_SELECTOR); cons = cons_el.get_text(strip=True) if cons_el else None; review_text = ""
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

async def scrape_capterra_async( product_url_str: str, start_date_filter_dt: Optional[datetime.date] = None, end_date_filter_dt: Optional[datetime.date] = None) -> Dict[str, Any]:
    overall_start_time = time.perf_counter(); parsed_url = urlparse(product_url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]; company_slug = path_segments[2] if len(path_segments) >= 3 and path_segments[0].lower() == "p" else "unknown-slug"
    page_source, product_name_selenium_guess = await _load_all_capterra_reviews_selenium(product_url_str, company_slug)
    if not page_source: return {"status": "error", "message": "Failed to load page content with Async Selenium-Driverless.", "data": None, "summary": {"product_url": product_url_str, "product_name_guess": product_name_selenium_guess}}
    thread_name_parse = f"CapterraParse-{company_slug[:12]}"
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
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
    title="Capterra Scraper API - Async Driverless Loader",
    description="Selenium-Driverless (async) loads all reviews, then BeautifulSoup parses. Uses DOM change wait.",
    version="1.3.4" # Incremented
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
SELENIUM_PAGE_TIMEOUT_S = 45 
SELENIUM_ELEMENT_FIND_TIMEOUT_S = 10 # Reduced default element find timeout
SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S = 5 # Shorter timeout for popup elements
SELENIUM_INTERACTION_TIMEOUT_S = 7 # Reduced interaction timeout
IFRAME_CONTENT_DOC_TIMEOUT_S = 5.0 # Timeout for getting iframe content document

INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(4.0, 5.5) 
LOADING_SPINNER_SELECTOR = 'svg[class*="s1xr3lbz"]'
AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S = 25

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
    "button[aria-label='Close' i]", "button[class*='modal__close' i]", "button[class*='CloseButton']",
    "button[aria-label*='Dismiss' i]", "button[title*='Dismiss' i]",
    "div[role='dialog'] button[class*='close' i]", "div[id^='ZN_'] button[aria-label='Close']",
    "button[id^='cookie-consent-accept']",
]

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
            """const elem = arguments[0]; if (!elem) return false; const style = window.getComputedStyle(elem);
            if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) < 0.1) return false;
            return !!(elem.offsetWidth || elem.offsetHeight || elem.getClientRects().length > 0);""", element )
    except Exception: return False

async def try_click(driver: driverless_webdriver.Chrome, locator: Optional[Tuple[By, str]] = None, element: Optional[driverless_webdriver.WebElement] = None, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S, thread_name: str = "DefaultThread") -> bool:
    el_to_click = element; action_description = str(locator) if locator else "provided_element"
    try:
        if not el_to_click and locator: el_to_click = await driver.find_element(locator[0], locator[1], timeout=timeout)
        if not el_to_click: return False
        try: await driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", el_to_click); await asyncio.sleep(random.uniform(0.2, 0.4))
        except Exception: pass
        el_id_attr = await el_to_click.get_attribute('id'); el_class_attr = await el_to_click.get_attribute('class')
        el_id = (el_id_attr or "").lower(); el_class = (el_class_attr or "").lower()
        use_js_click = "popup" in thread_name.lower() or "close" in thread_name.lower() or "onetrust" in el_id or "modal-close" in el_class
        if use_js_click: await driver.execute_script("arguments[0].click();", el_to_click)
        else: await el_to_click.click(move_to=True)
        return True
    except ElementClickInterceptedException:
        await asyncio.sleep(random.uniform(0.2, 0.4))
        try:
            if not el_to_click and locator: el_to_click = await driver.find_element(locator[0], locator[1], timeout=1)
            elif not el_to_click: return False
            await driver.execute_script("arguments[0].click();", el_to_click); return True
        except Exception as e_js: print(f"    [{thread_name}][try_click] JS fallback failed for {action_description}: {type(e_js).__name__} - {e_js}"); return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException, NoSuchElementException): return False
    except Exception as e_other: print(f"    [{thread_name}][try_click] Unexpected click error for {action_description}: {type(e_other).__name__} - {e_other}"); return False

async def attempt_to_close_popups_capterra(driver: driverless_webdriver.Chrome, thread_name: str):
    print(f"      [{thread_name}] Popup closer: Starting scan.")
    closed_any = False; main_window_handle = driver.current_window_handle
    specific_popup_selector = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"]'
    try:
        # print(f"      [{thread_name}] Popup closer: Finding specific popups with selector '{specific_popup_selector}' (timeout {SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S}s).")
        specific_popups = await driver.find_elements(By.CSS_SELECTOR, specific_popup_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
        # print(f"      [{thread_name}] Popup closer: Found {len(specific_popups)} specific popup(s).")
        for sp_btn in specific_popups:
            if await try_click(driver, element=sp_btn, timeout=SELENIUM_INTERACTION_TIMEOUT_S / 2, thread_name=f"{thread_name}-SpecificPopup"):
                closed_any = True; await asyncio.sleep(0.8); break
    except TimeoutException: print(f"      [{thread_name}] Popup closer: Timeout finding specific selector '{specific_popup_selector}'.")
    except Exception as e_spec: print(f"      [{thread_name}] Popup closer: Error checking specific selector: {type(e_spec).__name__}: {e_spec}")

    for sel_idx, sel_str in enumerate(POPUP_CLOSE_SELECTORS_CAPTERRA):
        if sel_str == specific_popup_selector and closed_any: continue
        current_closed_this_selector_type = False
        # print(f"      [{thread_name}] Popup closer: Processing general selector #{sel_idx}: '{sel_str}' (timeout {SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S}s).")
        try:
            # print(f"      [{thread_name}] Popup closer: Finding iframes for selector '{sel_str}'")
            iframe_elements = await driver.find_elements(By.TAG_NAME, "iframe", timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
            # print(f"      [{thread_name}] Popup closer: Found {len(iframe_elements)} iframe(s).")
            for iframe_idx, iframe_el in enumerate(iframe_elements):
                # print(f"      [{thread_name}] Popup closer: Processing iframe #{iframe_idx}.")
                try:
                    if not await async_is_element_displayed_js(driver, iframe_el): continue
                    await asyncio.sleep(0.15) # Brief pause for iframe context
                    # print(f"      [{thread_name}] Popup closer: Attempting to get content_document for iframe #{iframe_idx} (timeout {IFRAME_CONTENT_DOC_TIMEOUT_S}s).")
                    iframe_doc = await asyncio.wait_for(iframe_el.content_document, timeout=IFRAME_CONTENT_DOC_TIMEOUT_S)
                    # print(f"      [{thread_name}] Popup closer: Got content_document for iframe #{iframe_idx}. Finding elements for '{sel_str}'.")
                    popup_buttons_in_iframe = await iframe_doc.find_elements(By.CSS_SELECTOR, sel_str, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
                    for p_btn in popup_buttons_in_iframe:
                        if await try_click(driver, element=p_btn, timeout=SELENIUM_INTERACTION_TIMEOUT_S / 2, thread_name=f"{thread_name}-IframeP-{sel_idx}"):
                            closed_any = True; current_closed_this_selector_type = True; await asyncio.sleep(0.6); break
                    if current_closed_this_selector_type: break 
                except asyncio.TimeoutError: print(f"      [{thread_name}] Popup closer: Timeout getting content_document for iframe #{iframe_idx}.")
                except TimeoutException: print(f"      [{thread_name}] Popup closer: Timeout finding elements for '{sel_str}' in iframe #{iframe_idx}.")
                except Exception as e_iframe_interact: print(f"      [{thread_name}] Popup closer: Error interacting with iframe #{iframe_idx} ('{sel_str}'): {type(e_iframe_interact).__name__}: {e_iframe_interact}")
            if current_closed_this_selector_type: continue
        except TimeoutException: print(f"      [{thread_name}] Popup closer: Timeout finding iframes for selector '{sel_str}'.")
        except Exception as e_find_iframe: print(f"      [{thread_name}] Popup closer: Error finding/processing iframes for '{sel_str}': {type(e_find_iframe).__name__}: {e_find_iframe}")

        try:
            # print(f"      [{thread_name}] Popup closer: Finding elements for '{sel_str}' in main document (timeout {SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S}s).")
            popups_main = await driver.find_elements(By.CSS_SELECTOR, sel_str, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
            for p_btn in popups_main:
                if await try_click(driver, element=p_btn, timeout=SELENIUM_INTERACTION_TIMEOUT_S / 2, thread_name=f"{thread_name}-MainP-{sel_idx}"):
                    closed_any = True; current_closed_this_selector_type = True; await asyncio.sleep(0.6); break
        except TimeoutException: print(f"      [{thread_name}] Popup closer: Timeout finding elements for '{sel_str}' in main document.")
        except Exception as e_main_doc: print(f"      [{thread_name}] Popup closer: Error with main doc selector '{sel_str}': {type(e_main_doc).__name__}: {e_main_doc}")
    
    # print(f"      [{thread_name}] Popup closer: Finished selector scan. Checking window handles.")
    final_handles = set(await driver.window_handles); initial_handles_set = set([main_window_handle])
    if len(final_handles) > len(initial_handles_set):
        new_handles = final_handles - initial_handles_set
        current_main_handle_before_switch = driver.current_window_handle
        for handle_id_str in new_handles:
            try: await driver.switch_to.window(handle_id_str); await driver.close()
            except Exception: pass 
        try: 
            if current_main_handle_before_switch in await driver.window_handles: await driver.switch_to.window(current_main_handle_before_switch)
            elif main_window_handle in await driver.window_handles: await driver.switch_to.window(main_window_handle)
        except Exception: pass
    
    if closed_any: print(f"      [{thread_name}] Popup handling actions MAY have been taken (closed_any=True).")
    print(f"      [{thread_name}] Popup closer: Scan completed.")


async def async_wait_until_reviews_load_or_loader_gone(driver: driverless_webdriver.Chrome, initial_review_count: int, review_card_selector: str, loader_selector: str, timeout: float, thread_name: str ):
    loader_was_visible_after_click = False
    try:
        loader_elements = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
        for el in loader_elements:
            if await async_is_element_displayed_js(driver, el): loader_was_visible_after_click = True; break
    except TimeoutException: pass # OK if loader not found quickly
    except Exception: pass 
    start_time = time.monotonic()
    while time.monotonic() - start_time < timeout:
        current_reviews = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S) # Added timeout here too
        if len(current_reviews) > initial_review_count: return True
        if loader_was_visible_after_click:
            try:
                any_loader_displayed = False
                loader_elements_check = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
                for el in loader_elements_check:
                    if await async_is_element_displayed_js(driver, el): any_loader_displayed = True; break
                if not any_loader_displayed: 
                    await asyncio.sleep(0.25) 
                    current_reviews_after_loader = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
                    return len(current_reviews_after_loader) > initial_review_count or True 
            except (StaleElementReferenceException, NoSuchElementException): return True 
            except TimeoutException: pass # OK if loader elements disappear during check
        await asyncio.sleep(0.4) 
    current_reviews_after_timeout = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=SELENIUM_POPUP_ELEMENT_FIND_TIMEOUT_S)
    if len(current_reviews_after_timeout) > initial_review_count: return True
    raise TimeoutException(f"Timeout in async_wait. Initial: {initial_review_count}, Current: {len(current_reviews_after_timeout)}")

async def _load_all_capterra_reviews_selenium(product_url_str: str, company_slug: str) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoad-{company_slug[:15]}"; print(f"  [{thread_name}] Started Async Driverless loading for: {product_url_str}")
    product_name_guess = company_slug.replace("-"," ").title(); page_source_result: Optional[str] = None
    options = prepare_driverless_options()
    try:
        async with driverless_webdriver.Chrome(options=options) as driver:
            print(f"    [{thread_name}] Navigating to URL with timeout: {SELENIUM_PAGE_TIMEOUT_S}s")
            await driver.get(product_url_str, timeout=SELENIUM_PAGE_TIMEOUT_S)
            print(f"    [{thread_name}] Navigation complete. Sleeping for {INITIAL_PAGE_LOAD_SLEEP_S:.2f}s")
            await asyncio.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
            print(f"    [{thread_name}] Attempting to close popups after initial load...")
            await attempt_to_close_popups_capterra(driver, thread_name)
            print(f"    [{thread_name}] Popup closing attempt finished after initial load.")

            try:
                print(f"    [{thread_name}] Attempting to find product name H1..."); h1_el = await driver.find_element(By.CSS_SELECTOR, BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR, timeout=5)
                h1_text = await h1_el.text
                if "reviews of" in h1_text.lower(): product_name_guess = h1_text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title(); print(f"    [{thread_name}] Product name updated from H1: {product_name_guess}")
            except TimeoutException: print(f"    [{thread_name}] Product name H1 not found quickly.")
            except Exception as e_h1: print(f"    [{thread_name}] Minor error getting H1: {e_h1}")
            print(f"    [{thread_name}] Product Name Context: {product_name_guess}")
            show_more_clicks = 0; print(f"  [{thread_name}] Starting 'Show more reviews' loop...")
            try: 
                await driver.find_element(By.CSS_SELECTOR, REVIEW_CARDS_CONTAINER_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S) # Normal timeout here
                print(f"    [{thread_name}] Review cards container found.")
            except TimeoutException: print(f"  [{thread_name}] Review container not found initially."); page_source_result = await driver.page_source; return page_source_result, product_name_guess
            
            while True: # Main "Show More" loop
                initial_review_elements = await driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                initial_review_count_dom = len(initial_review_elements)
                try:
                    show_more_button_el = await driver.find_element(By.CSS_SELECTOR, SHOW_MORE_REVIEWS_BUTTON_SELECTOR, timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                    print(f"    [{thread_name}] Attempting to click 'Show More' (#{show_more_clicks + 1}). DOM reviews before: {initial_review_count_dom}.")
                    clicked = await try_click(driver, element=show_more_button_el, timeout=SELENIUM_INTERACTION_TIMEOUT_S, thread_name=thread_name)
                    if not clicked:
                        print(f"    [{thread_name}] try_click failed for 'Show More'. Ending loop."); break
                    
                    show_more_clicks += 1; await asyncio.sleep(random.uniform(0.4, 0.7))
                    print(f"      [{thread_name}] Click #{show_more_clicks} performed. Waiting for content update (max {AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S}s)...")
                    try:
                        await async_wait_until_reviews_load_or_loader_gone(driver, initial_review_count_dom, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", LOADING_SPINNER_SELECTOR, AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S, thread_name)
                        print(f"      [{thread_name}] Wait condition met for click #{show_more_clicks}.")
                    except TimeoutException:
                        print(f"      [{thread_name}] Timeout waiting for reviews after click #{show_more_clicks}. Fallback check.")
                        current_reviews_after_timeout_elements = await driver.find_elements(By.CSS_SELECTOR, f'{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}', timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                        if len(current_reviews_after_timeout_elements) == initial_review_count_dom: print(f"      [{thread_name}] Review count unchanged after timeout. Assuming end."); break 
                        else: print(f"      [{thread_name}] Reviews DID increase ({initial_review_count_dom} -> {len(current_reviews_after_timeout_elements)}) despite timeout.")
                    
                    await asyncio.sleep(random.uniform(0.4, 0.7))
                    # print(f"      [{thread_name}] Attempting to close popups after 'Show More' click #{show_more_clicks}...")
                    await attempt_to_close_popups_capterra(driver, thread_name) # Close popups again
                    # print(f"      [{thread_name}] Popup closing attempt finished after click #{show_more_clicks}.")

                    num_reviews_after_action_elements = await driver.find_elements(By.CSS_SELECTOR, f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}", timeout=SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                    if len(num_reviews_after_action_elements) == initial_review_count_dom and show_more_clicks > 0: print(f"    [{thread_name}] DOM review count ({len(num_reviews_after_action_elements)}) unchanged after full wait cycle. Assuming end."); break
                
                except TimeoutException: print(f"  [{thread_name}] 'Show more reviews' button not found in loop. Assuming all loaded."); break
                except Exception as e_sm_loop: print(f"  [{thread_name}] Error in 'Show more' loop: {type(e_sm_loop).__name__} - {e_sm_loop}"); traceback.print_exc(file=sys.stdout); await attempt_to_close_popups_capterra(driver, thread_name); await asyncio.sleep(0.5)
            
            print(f"  [{thread_name}] 'Show more' loop finished. Clicks: {show_more_clicks}. Retrieving page source."); page_source_result = await driver.page_source
            print(f"  [{thread_name}] Async Driverless session ended.")
        return page_source_result, product_name_guess
    except RuntimeError as e_setup: print(f"  [{thread_name}] CRITICAL DRIVER SETUP ERROR for {product_url_str}: {e_setup}"); traceback.print_exc(); return None, product_name_guess
    except Exception as e_load_main: print(f"  [{thread_name}] MAJOR ERROR during Async Driverless loading for {product_url_str}: {e_load_main}"); traceback.print_exc(); return None, product_name_guess

# --- Parsing functions (simplified for brevity, no changes from previous full code) ---
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
    print(f"Orchestrating ASYNC Capterra scrape for: {product_url_str} (Slug: {company_slug})")
    page_source, product_name_selenium_guess = await _load_all_capterra_reviews_selenium(product_url_str, company_slug)
    if not page_source: return {"status": "error", "message": "Failed to load page content with Async Selenium-Driverless.", "data": None, "summary": {"product_url": product_url_str, "product_name_guess": product_name_selenium_guess}}
    thread_name_parse = f"CapterraParse-{company_slug[:15]}"; print(f"  [{thread_name_parse}] Starting BS4 parsing phase...")
    parsed_data_obj = _parse_capterra_html_for_reviews(page_source, product_url_str, product_name_selenium_guess, thread_name_parse); parsed_data_obj.scrape_duration_seconds = round(time.perf_counter() - overall_start_time, 2)
    if start_date_filter_dt or end_date_filter_dt:
        filtered_reviews = []
        for review in parsed_data_obj.reviews:
            try:
                if not review.datetime: continue; review_date_str = review.datetime.split(" ")[0]; review_dt = datetime.strptime(review_date_str, "%Y-%m-%d").date()
                valid_start = not start_date_filter_dt or review_dt >= start_date_filter_dt; valid_end = not end_date_filter_dt or review_dt <= end_date_filter_dt
                if valid_start and valid_end: filtered_reviews.append(review)
            except ValueError: print(f"    [{thread_name_parse}] Warning: Could not parse date '{review.datetime}' for filtering."); continue
            except Exception as e_filter: print(f"    [{thread_name_parse}] Warning: Error filtering review by date '{review.datetime}': {e_filter}."); continue
        parsed_data_obj.reviews = filtered_reviews; parsed_data_obj.reviews_count_scraped = len(filtered_reviews); print(f"  [{thread_name_parse}] Applied date filters. Reviews after filtering: {len(filtered_reviews)}")
    return {"status": "success" if parsed_data_obj.reviews_count_scraped > 0 else "no_reviews_found_after_filter", "data": parsed_data_obj.model_dump(mode='json', by_alias=True), "summary": {"product_name": parsed_data_obj.product_name_scraped, "total_reviews_scraped": parsed_data_obj.reviews_count_scraped, "duration_seconds": parsed_data_obj.scrape_duration_seconds}}

@app.post("/scrape-capterra", tags=["Capterra"])
async def scrape_capterra_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    print(f"--- ASYNC RAW REQUEST DATA RECEIVED (v{app.version}) ---"); print(request.model_dump_json(indent=2)); print(f"--- END RAW REQUEST DATA ---")
    start_date_filter_dt, end_date_filter_dt = None, None 
    if request.start_date_str:
        try: start_date_filter_dt = datetime.strptime(request.start_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str format (YYYY-MM-DD).")
    if request.end_date_str:
        try: end_date_filter_dt = datetime.strptime(request.end_date_str, "%Y-%m-%d").date()
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str format (YYYY-MM-DD).")
    if start_date_filter_dt and end_date_filter_dt and start_date_filter_dt > end_date_filter_dt: raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
    if not request.urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    print(f"Async API request for Capterra: {len(request.urls)} URLs (v{app.version}).")
    results: Dict[str, Dict[str, Any]] = {}; valid_urls_for_processing = []; url_to_task_map = {} 
    for url_obj in request.urls:
        s_url = str(url_obj)
        if "capterra.com/p/" in s_url and "/reviews" in s_url: valid_urls_for_processing.append(url_obj)
        else: results[s_url] = {"status": "error", "message": f"Invalid Capterra URL format: {s_url} or skipped."}
    if not valid_urls_for_processing:
        if results: return results 
        raise HTTPException(status_code=400, detail="No valid Capterra URLs provided.")
    tasks = [asyncio.create_task(scrape_capterra_async(str(url), start_date_filter_dt, end_date_filter_dt), name=f"scrape_{str(url)}") for url in valid_urls_for_processing]
    for task, url_obj in zip(tasks, valid_urls_for_processing): url_to_task_map[task] = str(url_obj)
    print(f"  Created {len(tasks)} async scraping tasks.")
    task_execution_results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res_or_exc in enumerate(task_execution_results):
        original_url = url_to_task_map[tasks[i]] 
        if isinstance(res_or_exc, Exception):
            print(f"Task for {original_url} (v{app.version}) EXCEPTION: {type(res_or_exc).__name__} - {res_or_exc}"); traceback.print_exc(file=sys.stdout)
            results[original_url] = {"status": "error", "message": f"Scraping task failed: {type(res_or_exc).__name__}."}
        else: results[original_url] = res_or_exc 
    print(f"Finished ASYNC Capterra API request processing (v{app.version})."); return results
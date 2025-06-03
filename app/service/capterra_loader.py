import asyncio
import re
import time
import traceback
import random
from typing import Optional, Tuple

import selenium_driverless.webdriver as driverless_webdriver
from selenium_driverless.types.by import By
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from app.core import config
from app.utils.selenium_helpers import (
    prepare_driverless_driver,
    try_click_element,
    handle_specific_overlay_popup,
    async_wait_until_reviews_load
)

async def _get_total_reviews_on_site_selenium(driver: driverless_webdriver.Chrome, thread_name: str) -> Optional[int]:
    primary_selectors_with_pattern_check = [config.BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR, 'span[class*="sr2r3oj"]'] # Simplified
    try:
        print(f"      [{thread_name}] Initiating Selenium-based total review count extraction.")
        for i, selector in enumerate(primary_selectors_with_pattern_check):
            print(f"      [{thread_name}] Primary attempt (selector {i+1}): Trying '{selector}' with text pattern check.")
            try:
                candidate_spans = await driver.find_elements(By.CSS_SELECTOR, selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / (2 * len(primary_selectors_with_pattern_check)))
                if not candidate_spans and i == 0:
                    print(f"      [{thread_name}] No elements found with selector '{selector}'.")
                    continue
                for el_idx, el in enumerate(candidate_spans):
                    try:
                        text = await el.text
                        if not text: continue
                        match = re.search(r"([\d\.]+)\s*\((\d{1,3}(?:,\d{3})*|\d+)\)", text) # Rating (Reviews)
                        if match and match.group(2):
                            count_str = match.group(2).replace(",", "")
                            print(f"      [{thread_name}] Found matching pattern! Extracted total review count '{count_str}' from element {el_idx} (selector '{selector}') with text: '{text[:50]}...'")
                            return int(count_str)
                    except StaleElementReferenceException:
                        print(f"      [{thread_name}] Stale element encountered for selector '{selector}', element index {el_idx}. Skipping.")
                    except Exception as e_el_text:
                        print(f"      [{thread_name}] Error processing element {el_idx} for selector '{selector}': {type(e_el_text).__name__} - {e_el_text}. Skipping.")
            except TimeoutException:
                print(f"      [{thread_name}] Timeout for selector '{selector}'.")

        print(f"      [{thread_name}] All primary attempts with text pattern check failed to find total review count.")
        
        fallback_selector = config.BS_REVIEW_COUNT_DISPLAY_SELECTOR
        print(f"      [{thread_name}] Fallback attempt: Trying selector for review count display text: '{fallback_selector}'")
        try:
            count_display_candidates = await driver.find_elements(By.CSS_SELECTOR, fallback_selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
            for el in count_display_candidates:
                try:
                    text = await el.text
                    if not text: continue
                    # Try "X of Y Reviews" or "Showing X of Y Reviews"
                    match_of = re.search(r"(?:of|from)\s+([\d,]+)\s+Reviews", text, re.IGNORECASE)
                    if match_of:
                        count_str = match_of.group(1).replace(",", "")
                        print(f"      [{thread_name}] Extracted total review count '{count_str}' from 'of X Reviews' text: '{text[:50]}...'")
                        return int(count_str)
                    
                    # Try "X Reviews"
                    match_total_only = re.search(r"^([\d,]+)\s+Reviews$", text.strip(), re.IGNORECASE)
                    if match_total_only:
                        count_str = match_total_only.group(1).replace(",", "")
                        print(f"      [{thread_name}] Extracted total review count '{count_str}' from 'X Reviews' text: '{text[:50]}...'")
                        return int(count_str)
                except StaleElementReferenceException:
                    print(f"      [{thread_name}] Stale element encountered for fallback selector '{fallback_selector}'. Skipping.")
                except Exception as e_fb_el_text:
                    print(f"      [{thread_name}] Error processing element for fallback selector '{fallback_selector}': {type(e_fb_el_text).__name__} - {e_fb_el_text}. Skipping.")
        except TimeoutException:
            print(f"      [{thread_name}] Timeout for fallback selector '{fallback_selector}'.")

        print(f"      [{thread_name}] Total review count not found via any Selenium method (primary or fallback).")
        return None
    except Exception as e_general:
        print(f"      [{thread_name}] General error during Selenium total review count extraction: {type(e_general).__name__} - {e_general}")
        traceback.print_exc()
        return None

async def _load_all_capterra_reviews_driverless(product_url_str: str, company_slug: str) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoad-{company_slug[:10]}"
    print(f"  [{thread_name}] Starting Driverless for: {product_url_str}")
    product_name_guess = company_slug.replace("-"," ").title()
    page_source_result: Optional[str] = None
    driver: Optional[driverless_webdriver.Chrome] = None
    show_more_clicks = 0
    
    initial_sleep = random.uniform(config.INITIAL_PAGE_LOAD_SLEEP_S_MIN, config.INITIAL_PAGE_LOAD_SLEEP_S_MAX)

    try:
        driver = await prepare_driverless_driver()
        if not driver:
            return None, product_name_guess

        print(f"    [{thread_name}] Navigating to URL: {product_url_str}")
        await driver.get(product_url_str, timeout=config.SELENIUM_PAGE_TIMEOUT_S)
        print(f"    [{thread_name}] Navigation complete. Initial sleep: {initial_sleep:.2f}s")
        await asyncio.sleep(initial_sleep)

        await handle_specific_overlay_popup(driver, thread_name)

        try:
            await driver.find_element(By.CSS_SELECTOR, config.REVIEW_CARDS_CONTAINER_SELECTOR, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S)
        except TimeoutException:
            print(f"  [{thread_name}] Review container not found initially. Getting page source.")
            page_source_result = await driver.page_source
            return page_source_result, product_name_guess
        
        total_reviews_on_site = await _get_total_reviews_on_site_selenium(driver, thread_name)
        
        review_card_full_selector = f"{config.REVIEW_CARDS_CONTAINER_SELECTOR} {config.INDIVIDUAL_REVIEW_CARD_SELECTOR}"

        if total_reviews_on_site is not None and total_reviews_on_site > 0:
            print(f"    [{thread_name}] Determined strategy: Load {total_reviews_on_site} reviews.")
            initial_review_elements_after_load = await driver.find_elements(By.CSS_SELECTOR, review_card_full_selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
            reviews_loaded_initially = len(initial_review_elements_after_load)

            if reviews_loaded_initially == 0 and total_reviews_on_site > 0:
                print(f"    [{thread_name}] WARNING: No reviews found initially in DOM, but site indicates {total_reviews_on_site}. Proceeding with clicks.")
            
            if reviews_loaded_initially >= total_reviews_on_site:
                print(f"    [{thread_name}] All {total_reviews_on_site} reviews ({reviews_loaded_initially} in DOM) seem to be loaded initially. No clicks needed.")
                num_clicks_needed = 0
            else:
                reviews_remaining_to_load = total_reviews_on_site - reviews_loaded_initially
                num_clicks_needed = max(0, (reviews_remaining_to_load + config.REVIEWS_BATCH_SIZE - 1) // config.REVIEWS_BATCH_SIZE)
                print(f"    [{thread_name}] Calculated {num_clicks_needed} 'Show more' clicks needed (initially {reviews_loaded_initially}, remaining {reviews_remaining_to_load}).")

            for click_num in range(num_clicks_needed):
                current_review_elements_pre_click = await driver.find_elements(By.CSS_SELECTOR, review_card_full_selector, timeout=0.2)
                current_review_count_dom = len(current_review_elements_pre_click)
                print(f"    [{thread_name}] Click loop iter {click_num+1}/{num_clicks_needed}. Current DOM reviews: {current_review_count_dom} (Target: {total_reviews_on_site})")

                if current_review_count_dom >= total_reviews_on_site:
                    print(f"    [{thread_name}] DOM reviews ({current_review_count_dom}) meet/exceed target ({total_reviews_on_site}). Stopping clicks early.")
                    break
                
                if show_more_clicks > 60 and show_more_clicks % 7 == 0:
                    extra_sleep = random.uniform(1.5, 3.0)
                    print(f"      [{thread_name}] Adaptive sleep for {extra_sleep:.2f}s after {show_more_clicks} clicks.")
                    await asyncio.sleep(extra_sleep)
                elif show_more_clicks > 30 and show_more_clicks % 5 == 0:
                    extra_sleep = random.uniform(0.8, 1.8)
                    print(f"      [{thread_name}] Adaptive sleep for {extra_sleep:.2f}s after {show_more_clicks} clicks.")
                    await asyncio.sleep(extra_sleep)
                
                try:
                    print(f"      [{thread_name}] Attempting to find 'Show More' button...")
                    show_more_button_el = await driver.find_element(By.CSS_SELECTOR, config.SHOW_MORE_REVIEWS_BUTTON_SELECTOR, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                    print(f"      [{thread_name}] 'Show More' button found. Attempting click...")
                    if show_more_clicks > 25: await asyncio.sleep(random.uniform(0.2, 0.5))
                    
                    clicked = await try_click_element(driver, element=show_more_button_el, thread_name=thread_name, always_js_click=True) # Force JS click based on observation from original
                    if not clicked:
                        print(f"    [{thread_name}] try_click (forced JS) failed for 'Show More' on click {show_more_clicks + 1}. Breaking loop.")
                        break
                    
                    show_more_clicks += 1
                    reviews_loaded_successfully = await async_wait_until_reviews_load(driver, current_review_count_dom, review_card_full_selector, config.LOADING_SPINNER_SELECTOR, config.AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S, thread_name)
                    if not reviews_loaded_successfully:
                        print(f"      [{thread_name}] Wait condition indicated no new reviews loaded after click #{show_more_clicks} (iter {click_num+1}). Breaking loop.")
                        break
                    
                    if show_more_clicks > 0 and show_more_clicks % 7 == 0 : # Re-check for popups periodically
                        await handle_specific_overlay_popup(driver, thread_name)

                except TimeoutException:
                    print(f"  [{thread_name}] 'Show more' button not found (before click {show_more_clicks +1} of {num_clicks_needed}). Assuming all loaded or error.")
                    break
                except Exception as e_sm_loop:
                    print(f"  [{thread_name}] Error in 'Show more' loop (click {show_more_clicks +1}): {type(e_sm_loop).__name__} - {e_sm_loop}")
                    break
            
            final_reviews_in_dom = await driver.find_elements(By.CSS_SELECTOR, review_card_full_selector, timeout=0.2)
            print(f"    [{thread_name}] Determined click strategy finished. Clicks: {show_more_clicks}. Reviews in DOM: {len(final_reviews_in_dom)} (Site indicates: {total_reviews_on_site})")

        else: # Fallback logic if total_reviews_on_site is None or 0
            print(f"    [{thread_name}] Could not determine total review count or site indicates 0. Falling back to max_show_more_attempts (heuristic) logic.")
            max_show_more_attempts = 250 # Max attempts if total not known
            for attempt_loop in range(max_show_more_attempts):
                initial_review_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_full_selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
                initial_review_count_dom = len(initial_review_elements)

                if show_more_clicks > 60 and show_more_clicks % 7 == 0:
                    extra_sleep = random.uniform(1.5, 3.0)
                    print(f"      [{thread_name}][Fallback] Adaptive sleep for {extra_sleep:.2f}s after {show_more_clicks} clicks.")
                    await asyncio.sleep(extra_sleep)
                elif show_more_clicks > 30 and show_more_clicks % 5 == 0:
                    extra_sleep = random.uniform(0.8, 1.8)
                    print(f"      [{thread_name}][Fallback] Adaptive sleep for {extra_sleep:.2f}s after {show_more_clicks} clicks.")
                    await asyncio.sleep(extra_sleep)

                try:
                    print(f"      [{thread_name}][Fallback] Attempting to find 'Show More' button...")
                    show_more_button_el = await driver.find_element(By.CSS_SELECTOR, config.SHOW_MORE_REVIEWS_BUTTON_SELECTOR, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S)
                    print(f"      [{thread_name}][Fallback] 'Show More' button found. Attempting click...")
                    if show_more_clicks > 25: await asyncio.sleep(random.uniform(0.2, 0.5))
                    
                    clicked = await try_click_element(driver, element=show_more_button_el, thread_name=thread_name, always_js_click=True)
                    if not clicked:
                        print(f"    [{thread_name}][Fallback] try_click failed for 'Show More' on attempt {show_more_clicks + 1}. Assuming end.")
                        break
                    
                    show_more_clicks += 1
                    reviews_loaded_successfully = await async_wait_until_reviews_load(driver, initial_review_count_dom, review_card_full_selector, config.LOADING_SPINNER_SELECTOR, config.AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S, thread_name)
                    if not reviews_loaded_successfully:
                        print(f"      [{thread_name}][Fallback] Wait condition indicated no new reviews loaded after click #{show_more_clicks}. Assuming end.")
                        break
                    
                    if show_more_clicks > 0 and show_more_clicks % 7 == 0:
                        await handle_specific_overlay_popup(driver, thread_name)
                except TimeoutException:
                    print(f"  [{thread_name}][Fallback] 'Show more' button not found (iter {attempt_loop+1}). Assuming all loaded.")
                    break
                except Exception as e_sm_loop:
                    print(f"  [{thread_name}][Fallback] Error in 'Show more' loop (iter {attempt_loop+1}): {type(e_sm_loop).__name__} - {e_sm_loop}")
                    break
            print(f"    [{thread_name}][Fallback] Heuristic loop finished. Clicks: {show_more_clicks}.")

        print(f"  [{thread_name}] 'Show more' process completed. Total clicks: {show_more_clicks}. Retrieving page source.")
        page_source_result = await driver.page_source

    except RuntimeError as e_setup:
        print(f"  [{thread_name}] CRITICAL DRIVER SETUP ERROR: {e_setup}")
        traceback.print_exc()
    except Exception as e_load_main:
        print(f"  [{thread_name}] MAJOR ERROR during Driverless loading: {type(e_load_main).__name__} - {e_load_main}")
        traceback.print_exc()
    finally:
        if driver:
            print(f"  [{thread_name}] Attempting to quit driver.")
            try:
                await driver.quit()
                print(f"  [{thread_name}] Driver quit successfully.")
            except Exception as e_quit:
                print(f"  [{thread_name}] Error during driver.quit(): {type(e_quit).__name__} - {e_quit}")
    
    return page_source_result, product_name_guess
import asyncio 
import sys

if sys.platform == "win32" and sys.version_info >= (3, 8):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        print("INFO: Applied WindowsSelectorEventLoopPolicy for asyncio.")
    except Exception as e_policy:
        print(f"WARNING: Could not set WindowsSelectorEventLoopPolicy: {e_policy}")

import os
import json
import re
import time
import random
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

from pydantic import BaseModel, Field, HttpUrl, ValidationError
from fastapi import FastAPI, HTTPException, Body
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, BrowserContext, Error as PlaywrightError

# --- Pydantic Models ---
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

app = FastAPI(
    title="Capterra Scraper API - Playwright Loader",
    description="Playwright loads all reviews, then BeautifulSoup parses. Uses JS for wait condition.",
    version="2.0.3" 
)

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants for Capterra ---
PLAYWRIGHT_PAGE_TIMEOUT_MS = 60 * 1000 
PLAYWRIGHT_NAVIGATION_TIMEOUT_MS = 45 * 1000 
PLAYWRIGHT_ELEMENT_TIMEOUT_MS = 20 * 1000 
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(3.0, 5.0) 
AFTER_SHOW_MORE_CLICK_WAIT_TIMEOUT_S = 25 

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

async def attempt_to_close_popups_playwright(page: Page, thread_name: str):
    closed_any = False
    specific_popup_selector = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"]'
    try:
        specific_close_button = page.locator(specific_popup_selector).first
        if await specific_close_button.is_visible(timeout=1500) and await specific_close_button.is_enabled(timeout=500):
            await specific_close_button.click(timeout=2000, force=True)
            closed_any = True; await page.wait_for_timeout(500)
    except Exception: pass 
    for sel in POPUP_CLOSE_SELECTORS_CAPTERRA:
        if sel == specific_popup_selector and closed_any: continue 
        popup_closed_this_pass = False
        for frame in page.frames:
            if frame.is_detached(): continue
            try:
                btn = frame.locator(sel).first
                if await btn.is_visible(timeout=300) and await btn.is_enabled(timeout=150):
                    await btn.click(timeout=1000, force=True); closed_any = True; popup_closed_this_pass = True; await page.wait_for_timeout(300)
                    break
            except Exception: pass
            if popup_closed_this_pass: break 
        if popup_closed_this_pass and not await page.locator(sel).is_visible(timeout=100): continue
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=300) and await btn.is_enabled(timeout=150):
                 await btn.click(timeout=1000, force=True); closed_any = True; popup_closed_this_pass = True; await page.wait_for_timeout(300)
                 if not await page.locator(sel).is_visible(timeout=100): break
        except Exception: pass
    if closed_any: print(f"      [{thread_name}] Popup handling actions possibly taken."); await page.wait_for_timeout(random.randint(300, 700))

def parse_capterra_datetime_for_output(date_str: str) -> Tuple[Optional[str], Optional[datetime]]:
    if not date_str: return None, None
    parsed_dt = None
    formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try: parsed_dt = datetime.strptime(date_str.strip(), fmt); break
        except ValueError: continue
    if parsed_dt: return parsed_dt.strftime("%Y-%m-%d") + " 00:00:00 +0000", parsed_dt 
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
                 if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', cand.get_text(strip=True), re.IGNORECASE):
                    date_el_found = cand; break
            if not date_el_found:
                current_el = title_block_parent_scope
                for _ in range(2): 
                    if not current_el: break
                    for sib in current_el.find_next_siblings('div', class_='typo-0 text-neutral-90', limit=2):
                        if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', sib.get_text(strip=True), re.IGNORECASE):
                            date_el_found = sib; break
                    if date_el_found: break; current_el = current_el.parent
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
            p_tags = main_content_block.find_all('p', recursive=False); candidate_texts = []
            for p_tag in p_tags:
                p_text_content = p_tag.get_text(strip=True)
                is_pros_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Pros", re.I))))
                is_cons_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and p_tag.find_previous_sibling('span', string=re.compile(r"Cons", re.I))))
                if p_text_content and p_text_content != pros and p_text_content != cons and not is_pros_p and not is_cons_p:
                    candidate_texts.append(p_text_content)
            if candidate_texts: review_text = " ".join(candidate_texts)
        return CapterraIndividualReview(title=title, text=review_text, reviewer=reviewer_name, time_used_product=time_used, reviewer_avatar=reviewer_avatar_url, datetime=datetime_str_output, rating=rating_str, pros=pros, cons=cons)
    except Exception: return None

def _parse_capterra_html_for_reviews(page_source: str, original_url_str: str, selenium_product_name_guess: str, thread_name: str) -> CapterraScrapeResultOutput:
    soup = BeautifulSoup(page_source, DEFAULT_HTML_PARSER)
    parsed_reviews_list: List[CapterraIndividualReview] = []
    product_name_scraped = selenium_product_name_guess; product_category_scraped = None
    h1_title_el = soup.select_one(BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR)
    if h1_title_el and "reviews of" in h1_title_el.text.lower(): product_name_scraped = h1_title_el.text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
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
        if match: overall_rating_str, review_count_from_display = match.group(1), int(match.group(2)) if match.group(2) else None
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
        def get_rating(sel): el = summary_section.select_one(sel); return el.get_text(strip=True).split()[0] if el and el.get_text(strip=True) else None
        ease_of_use_str,customer_service_str,features_str,value_money_str = get_rating(BS_EASE_OF_USE_TOTAL_RATING_SELECTOR),get_rating(BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR),get_rating(BS_FEATURES_TOTAL_RATING_SELECTOR),get_rating(BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR)
    totals = CapterraReviewTotals(review_count=review_count_from_display, overall_rating=overall_rating_str, ease_of_use_rating=ease_of_use_str, customer_service_rating=customer_service_str, functionality_rating=features_str, value_for_money_rating=value_money_str)
    review_cards_container = soup.select_one(REVIEW_CARDS_CONTAINER_SELECTOR)
    if review_cards_container:
        review_card_soups = review_cards_container.select(INDIVIDUAL_REVIEW_CARD_SELECTOR)
        for card_soup in review_card_soups:
            review = _parse_individual_review_card_revised(card_soup, thread_name)
            if review: parsed_reviews_list.append(review)
    else: print(f"  [{thread_name}] Review cards container not found by BS4.")
    return CapterraScrapeResultOutput(totals=totals, reviews=parsed_reviews_list,product_name_scraped=product_name_scraped, product_category_scraped=product_category_scraped,original_url=HttpUrl(original_url_str), reviews_count_scraped=len(parsed_reviews_list),scrape_duration_seconds=0 )

async def _load_all_capterra_reviews_playwright_core( 
    product_url_str: str, company_slug: str, browser_context: BrowserContext
) -> Tuple[Optional[str], str]:
    thread_name = f"CapterraLoadPW-{company_slug[:12]}"
    page_content: Optional[str] = None
    page: Optional[Page] = None 
    product_name_guess = company_slug.replace("-"," ").title() 

    PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR = f"{REVIEW_CARDS_CONTAINER_SELECTOR} {INDIVIDUAL_REVIEW_CARD_SELECTOR}" # Use space for descendant

    try:
        if browser_context is None:
            print(f"  [{thread_name}] FATAL: BrowserContext is None in _core function!")
            return None, product_name_guess
            
        page = await browser_context.new_page()
        if page is None:
             print(f"  [{thread_name}] FATAL: Playwright page object is None after new_page() call!")
             return None, product_name_guess
            
        print(f"  [{thread_name}] Navigating to {product_url_str} with Playwright...")
        await page.goto(product_url_str, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAVIGATION_TIMEOUT_MS) 
        
        print(f"  [{thread_name}] Page loaded (domcontentloaded). Initial sleep for {INITIAL_PAGE_LOAD_SLEEP_S:.2f}s...")
        await page.wait_for_timeout(INITIAL_PAGE_LOAD_SLEEP_S * 1000)
        await attempt_to_close_popups_playwright(page, thread_name)
        try:
            h1_locator = page.locator(BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR).first
            await h1_locator.wait_for(state="visible", timeout=3000)
            h1_text = await h1_locator.text_content()
            if h1_text and "reviews of" in h1_text.lower():
                product_name_guess = h1_text.lower().replace("reviews of", "").replace("<!-- -->","").strip().title()
        except Exception: pass
        print(f"    [{thread_name}] Playwright phase - Product Name Context: {product_name_guess}")
        show_more_clicks = 0
        print(f"  [{thread_name}] Starting 'Show more reviews' loop with Playwright...")
        try:
            await page.locator(REVIEW_CARDS_CONTAINER_SELECTOR).wait_for(state="attached", timeout=PLAYWRIGHT_ELEMENT_TIMEOUT_MS)
            print(f"    [{thread_name}] Review cards container found.")
        except Exception:
            print(f"  [{thread_name}] Review container ('{REVIEW_CARDS_CONTAINER_SELECTOR}') not found. No reviews to load or page structure issue.")
            page_content = await page.content(); return page_content, product_name_guess
        while True:
            initial_review_count_dom = await page.locator(PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR).count()
            try:
                show_more_button = page.locator(SHOW_MORE_REVIEWS_BUTTON_SELECTOR).first
                await show_more_button.wait_for(state="visible", timeout=PLAYWRIGHT_ELEMENT_TIMEOUT_MS / 2)
                if not await show_more_button.is_enabled(timeout=1000): print(f"    [{thread_name}] 'Show more' button not enabled."); break
                print(f"    [{thread_name}] Clicking 'Show More' (#{show_more_clicks + 1}). DOM reviews before: {initial_review_count_dom}.")
                await show_more_button.scroll_into_view_if_needed(timeout=2000)
                await page.wait_for_timeout(random.randint(50,150))
                
                await show_more_button.click(timeout=PLAYWRIGHT_ELEMENT_TIMEOUT_MS / 3, force=True)
                show_more_clicks += 1
                
                print(f"      [{thread_name}] Click initiated. Waiting for review count to increase (max {AFTER_SHOW_MORE_CLICK_WAIT_TIMEOUT_S}s)...")
                
                js_review_card_selector_for_function = PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR.replace('"', '\\"')
                js_expression = f"""
                    () => {{
                        const initialCount = {initial_review_count_dom};
                        const currentElements = document.querySelectorAll("{js_review_card_selector_for_function}");
                        return currentElements.length > initialCount;
                    }}
                """
                try:
                    await page.wait_for_function(js_expression, timeout=AFTER_SHOW_MORE_CLICK_WAIT_TIMEOUT_S * 1000)
                    await page.wait_for_timeout(200) 
                    num_reviews_after_action = await page.locator(PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR).count()
                    print(f"      [{thread_name}] Review count increased to {num_reviews_after_action}.")
                except PlaywrightError: 
                    await page.wait_for_timeout(100) 
                    num_reviews_after_action = await page.locator(PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR).count()
                    print(f"      [{thread_name}] Timeout waiting for review count increase via JS. Before: {initial_review_count_dom}, After: {num_reviews_after_action}.")
                    if num_reviews_after_action == initial_review_count_dom: print(f"      [{thread_name}] Count unchanged. Assuming end/issue."); break 
                
                await page.wait_for_timeout(random.randint(100, 300))
                await attempt_to_close_popups_playwright(page, thread_name)
                
                current_review_count = await page.locator(PLAYWRIGHT_INDIVIDUAL_REVIEW_CARD_LOCATOR).count()
                if current_review_count == initial_review_count_dom and show_more_clicks > 0: # Check if count truly didn't change after everything
                    print(f"    [{thread_name}] DOM count stable after full wait cycle. Assuming end.")
                    break

            except PlaywrightError as e_pw_loop_timeout: 
                 print(f"  [{thread_name}] 'Show more reviews' button Playwright Timeout in loop. Assuming all loaded.")
                 break
            except Exception as e_sm: 
                print(f"  [{thread_name}] Generic error in 'Show more' loop: {type(e_sm).__name__} - {e_sm}")
                traceback.print_exc(file=sys.stdout); break 
        
        print(f"  [{thread_name}] 'Show more' loop finished. Clicks: {show_more_clicks}. Retrieving content.")
        page_content = await page.content()
        return page_content, product_name_guess

    except Exception as e_core_load:
        print(f"  [{thread_name}] Error in Playwright core loading ({product_url_str}): {type(e_core_load).__name__} - {e_core_load}")
        traceback.print_exc()
        if page:
            try: page_content = await page.content() 
            except: pass
        return page_content, product_name_guess 
    finally:
        if page: 
            try: await page.close()
            except Exception as e_page_close: print(f"  [{thread_name}] Error closing page: {e_page_close}")

async def _load_all_capterra_reviews_playwright_entry(
    product_url_str: str, company_slug: str
) -> Tuple[Optional[str], str]:
    async with async_playwright() as playwright_instance:
        browser = None; context = None
        product_name_guess = company_slug.replace("-"," ").title() 
        try:
            browser = await playwright_instance.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(java_script_enabled=True, accept_downloads=False)
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return await _load_all_capterra_reviews_playwright_core(product_url_str, company_slug, context)
        except Exception as e:
            print(f"  [Playwright Entry] Error setting up Playwright for {product_url_str}: {type(e).__name__} - {e}")
            return None, product_name_guess
        finally:
            if context: 
                try: await context.close()
                except Exception as e_ctx: print(f"Error closing context: {e_ctx}")
            if browser: 
                try: await browser.close()
                except Exception as e_brw: print(f"Error closing browser: {e_brw}")


def scrape_capterra_sync_wrapper(
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
    page_source, product_name_playwright_guess = asyncio.run(
        _load_all_capterra_reviews_playwright_entry(product_url_str, company_slug)
    )
    if not page_source:
        return {"status": "error", "message": "Failed to load page with Playwright.", 
                "data": None, "summary": {"product_url": product_url_str, "product_name_guess": product_name_playwright_guess}}
    thread_name_parse = f"CapterraParse-{company_slug[:15]}"
    print(f"  [{thread_name_parse}] Starting BS4 parsing phase...")
    parsed_data_obj = _parse_capterra_html_for_reviews(page_source, product_url_str, product_name_playwright_guess, thread_name_parse)
    parsed_data_obj.scrape_duration_seconds = round(time.perf_counter() - overall_start_time, 2)
    return {"status": "success" if parsed_data_obj.reviews_count_scraped > 0 else "no_reviews_found", 
            "data": parsed_data_obj.model_dump(mode='json', by_alias=True), 
            "summary": {"product_name": parsed_data_obj.product_name_scraped, 
                        "total_reviews_scraped": parsed_data_obj.reviews_count_scraped, 
                        "duration_seconds": parsed_data_obj.scrape_duration_seconds}}

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
    max_workers = min(len(request.urls), (os.cpu_count() or 1) * 2, 2) 
    print(f"  Using {max_workers} worker threads for Playwright tasks.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {}
        for url_obj in request.urls:
            url_str = str(url_obj)
            if "capterra.com/p/" not in url_str or "/reviews" not in url_str:
                 results[url_str] = {"status": "error", "message": f"Invalid Capterra URL format: {url_str}."}; continue
            future = executor.submit(scrape_capterra_sync_wrapper, url_str, start_date_filter_dt, end_date_filter_dt)
            future_to_url[future] = url_str
        for future in as_completed(future_to_url):
            original_url_str = future_to_url[future]
            try: results[original_url_str] = future.result()
            except Exception as e:
                print(f"Task for {original_url_str} (Capterra v{app.version}) EXCEPTION in executor: {e}")
                if isinstance(e, NotImplementedError): print("ERROR: asyncio.NotImplementedError. Ensure event loop policy is set for Windows if applicable.")
                traceback.print_exc(file=sys.stdout)
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(e).__name__} - {e}."}
    print(f"Finished Capterra API request processing (v{app.version}).")
    return results

# if __name__ == "__main__":
#     async def main_test_capterra():
#         test_url_google_pay = "https://www.capterra.com/p/212826/Google-Pay/reviews/" 
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url_google_pay)])
#         results = await scrape_capterra_endpoint(test_request)
#         print(json.dumps(results, indent=2, default=str))
#     asyncio.run(main_test_capterra())
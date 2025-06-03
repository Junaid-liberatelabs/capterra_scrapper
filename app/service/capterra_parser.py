import re
from typing import List, Optional
import traceback # Make sure traceback is imported for logging

from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from app.core import config
from app.schema.capterra_models import (
    CapterraReviewTotals,
    CapterraIndividualReview,
    CapterraScrapeResultOutput
)
from app.utils.parsing_helpers import parse_capterra_datetime_for_output

def _parse_individual_review_card_revised(review_card_soup: Tag, thread_name: str) -> Optional[CapterraIndividualReview]:
    try:
        reviewer_name: Optional[str] = None
        reviewer_avatar_url: Optional[HttpUrl] = None
        time_used: Optional[str] = None

        reviewer_name_el = review_card_soup.select_one(config.BS_REVIEWER_NAME_SELECTOR)
        if reviewer_name_el:
            reviewer_name = reviewer_name_el.get_text(strip=True)

        avatar_el = review_card_soup.select_one(config.BS_REVIEWER_AVATAR_IMG_SELECTOR)
        if avatar_el and avatar_el.has_attr('src'):
            try:
                reviewer_avatar_url = HttpUrl(avatar_el['src'])
            except Exception: 
                 reviewer_avatar_url = None

        if not reviewer_name: 
            initials_el = review_card_soup.select_one(config.BS_REVIEWER_INITIALS_FALLBACK_SELECTOR)
            if initials_el:
                reviewer_name = initials_el.get_text(strip=True)
        
        details_container = review_card_soup.select_one(config.BS_REVIEWER_INFO_CONTAINER_SELECTOR)
        if details_container:
            all_details_text = details_container.get_text(separator="\n", strip=True)
            time_used_match = re.search(r"Used the software for:\s*(.+)", all_details_text, re.IGNORECASE)
            if time_used_match:
                time_used = time_used_match.group(1).strip().rstrip('.')
        
        title_el = review_card_soup.select_one(config.BS_REVIEW_TITLE_SELECTOR)
        title = title_el.get_text(strip=True) if title_el else "No Title"

        date_str_on_page = "Unknown Date"
        date_el_found = None
        title_block_parent_scope = review_card_soup.select_one(f'{config.BS_REVIEW_TITLE_SELECTOR}')
        if title_block_parent_scope: title_block_parent_scope = title_block_parent_scope.parent 

        if title_block_parent_scope:
            date_candidates = title_block_parent_scope.find_all('div', class_='typo-0 text-neutral-90', recursive=False) 
            for cand in date_candidates:
                 if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', cand.get_text(strip=True), re.IGNORECASE):
                     date_el_found = cand
                     break
            if not date_el_found: 
                search_scopes = [details_container, title_block_parent_scope] 
                for scope in search_scopes:
                    if scope:
                        for sib in scope.find_next_siblings('div', class_='typo-0 text-neutral-90', limit=2):
                             if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', sib.get_text(strip=True), re.IGNORECASE):
                                date_el_found = sib
                                break
                        if date_el_found: break
                        child_dates = scope.select('div.typo-0.text-neutral-90') 
                        for cd in child_dates:
                            if re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2},\s*\d{4})', cd.get_text(strip=True), re.IGNORECASE):
                                date_el_found = cd
                                break
                        if date_el_found: break
        
        if date_el_found:
            date_str_on_page = date_el_found.get_text(strip=True)

        datetime_str_output, _ = parse_capterra_datetime_for_output(date_str_on_page)

        rating_el = review_card_soup.select_one(config.BS_REVIEW_CARD_OVERALL_RATING_SELECTOR)
        rating_str = rating_el.get_text(strip=True) if rating_el else "0.0"

        pros_el = review_card_soup.select_one(config.BS_REVIEW_PROS_SELECTOR)
        pros = pros_el.get_text(strip=True) if pros_el else None

        cons_el = review_card_soup.select_one(config.BS_REVIEW_CONS_SELECTOR)
        cons = cons_el.get_text(strip=True) if cons_el else None

        review_text = ""
        main_content_block = review_card_soup.select_one('div[class*="!mt-4 space-y-6"]')
        if main_content_block:
            p_tags = main_content_block.find_all('p', recursive=False)
            candidate_texts = []
            for p_tag in p_tags:
                p_text_content = p_tag.get_text(strip=True)

                # Original v10.py logic for checking if a <p> tag is part of Pros/Cons section
                is_pros_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and \
                                                   p_tag.find_previous_sibling('span', string=re.compile(r"Pros", re.I))))
                is_cons_p = bool(p_tag.find_parent('div', class_=lambda x: x and 'space-y-2' in x and \
                                                   p_tag.find_previous_sibling('span', string=re.compile(r"Cons", re.I))))

                # If the p_tag itself is the direct pros_el or cons_el, also exclude it
                # This handles cases where pros/cons are simple <p> tags not nested deeper
                if pros_el and p_tag == pros_el:
                    is_pros_p = True
                if cons_el and p_tag == cons_el:
                    is_cons_p = True
                
                if p_text_content and p_text_content != pros and p_text_content != cons and \
                   not is_pros_p and not is_cons_p:
                    if p_text_content.lower().startswith("overall experience:"):
                        p_text_content = p_text_content[len("overall experience:") :].strip()
                    if p_text_content:
                        candidate_texts.append(p_text_content)
            if candidate_texts:
                review_text = " ".join(candidate_texts)

        return CapterraIndividualReview(
            title=title,
            text=review_text,
            reviewer=reviewer_name,
            time_used_product=time_used,
            reviewer_avatar=reviewer_avatar_url,
            datetime=datetime_str_output,
            rating=rating_str,
            pros=pros,
            cons=cons
        )
    except Exception as e:
        print(f"    [{thread_name}] Error parsing individual review card: {type(e).__name__} - {e}")
        traceback.print_exc() 
        return None


def _parse_capterra_html_for_reviews(page_source: str, original_url_str: str, selenium_product_name_guess: str, thread_name: str) -> CapterraScrapeResultOutput:
    soup = BeautifulSoup(page_source, config.DEFAULT_HTML_PARSER)
    parsed_reviews_list: List[CapterraIndividualReview] = []
    
    product_name_scraped = selenium_product_name_guess # Fallback
    product_category_scraped: Optional[str] = None

    # Product Name
    h1_title_el = soup.select_one(config.BS_PRODUCT_NAME_FALLBACK_H1_SELECTOR) 
    if h1_title_el:
        h1_text = h1_title_el.get_text(strip=True)
        name_match = re.match(r"^(.*?)(\s+Reviews)?$", h1_text, re.IGNORECASE)
        if name_match and name_match.group(1):
            product_name_scraped = name_match.group(1).replace("<!-- -->","").strip().title()
    else: 
        name_header_el = soup.select_one(config.BS_PRODUCT_NAME_HEADER_SELECTOR)
        if name_header_el:
            product_name_scraped = name_header_el.get_text(strip=True)

    # Product Category
    cat_el = soup.select_one(config.BS_PRODUCT_CATEGORY_BREADCRUMB_SELECTOR)
    if cat_el:
        product_category_scraped = cat_el.get_text(strip=True)

    # Totals
    overall_rating_str: Optional[str] = None
    ease_of_use_str: Optional[str] = None
    customer_service_str: Optional[str] = None
    features_str: Optional[str] = None
    value_money_str: Optional[str] = None
    review_count_from_display: Optional[int] = None

    overall_product_rating_el = soup.select_one(config.BS_PRODUCT_OVERALL_RATING_HEADER_SELECTOR)
    if overall_product_rating_el:
        text_content = overall_product_rating_el.get_text(strip=True)
        match = re.match(r"([\d\.]+)(?:\s*\((\d{1,3}(?:,\d{3})*|\d+)\))?", text_content) 
        if match:
            overall_rating_str = match.group(1)
            if match.group(2):
                review_count_from_display = int(match.group(2).replace(",", ""))
    
    if not review_count_from_display: 
        review_count_display_el = soup.select_one(config.BS_REVIEW_COUNT_DISPLAY_SELECTOR)
        if review_count_display_el:
            text = review_count_display_el.get_text(strip=True) 
            count_match_of = re.search(r"(?:of|from)\s+([\d,]+)\s+Reviews", text, re.IGNORECASE)
            if count_match_of:
                review_count_from_display = int(count_match_of.group(1).replace(",", ""))
            else:
                count_match_showing_only = re.search(r"Showing\s+\d+(?:-\d+)?\s+(?:of|from)\s+([\d,]+)", text, re.IGNORECASE) 
                if count_match_showing_only:
                     review_count_from_display = int(count_match_showing_only.group(1).replace(",", ""))
                else:
                    count_match_total_only = re.search(r"^([\d,]+)\s+Reviews$", text.strip(), re.IGNORECASE)
                    if count_match_total_only:
                        review_count_from_display = int(count_match_total_only.group(1).replace(",", ""))


    summary_section = soup.select_one(config.BS_PRODUCT_RATING_SUMMARY_SECTION_SELECTOR)
    if summary_section:
        def get_rating_from_summary(selector: str) -> Optional[str]:
            el = summary_section.select_one(selector)
            return el.get_text(strip=True).split()[0] if el and el.get_text(strip=True) else None
        
        ease_of_use_str = get_rating_from_summary(config.BS_EASE_OF_USE_TOTAL_RATING_SELECTOR)
        customer_service_str = get_rating_from_summary(config.BS_CUSTOMER_SERVICE_TOTAL_RATING_SELECTOR)
        features_str = get_rating_from_summary(config.BS_FEATURES_TOTAL_RATING_SELECTOR)
        value_money_str = get_rating_from_summary(config.BS_VALUE_FOR_MONEY_TOTAL_RATING_SELECTOR)

    totals = CapterraReviewTotals(
        review_count=review_count_from_display,
        overall_rating=overall_rating_str,
        ease_of_use_rating=ease_of_use_str,
        customer_service_rating=customer_service_str,
        functionality_rating=features_str, 
        value_for_money_rating=value_money_str
    )

    # Individual Reviews
    review_cards_container = soup.select_one(config.REVIEW_CARDS_CONTAINER_SELECTOR)
    if review_cards_container:
        review_card_soups = review_cards_container.select(config.INDIVIDUAL_REVIEW_CARD_SELECTOR)
        for card_soup in review_card_soups:
            review = _parse_individual_review_card_revised(card_soup, thread_name)
            if review:
                parsed_reviews_list.append(review)
    else:
        print(f"  [{thread_name}] Review cards container ('{config.REVIEW_CARDS_CONTAINER_SELECTOR}') not found for BS4 parsing.")

    return CapterraScrapeResultOutput(
        totals=totals,
        reviews=parsed_reviews_list,
        product_name_scraped=product_name_scraped,
        product_category_scraped=product_category_scraped,
        original_url=HttpUrl(original_url_str),
        reviews_count_scraped=len(parsed_reviews_list),
        scrape_duration_seconds=0 
    )
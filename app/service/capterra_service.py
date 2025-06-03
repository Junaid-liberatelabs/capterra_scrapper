import time
import asyncio # Required for asyncio.current_task() in loader
from datetime import datetime # For type hinting if date filtering is added later
from urllib.parse import urlparse
from typing import Dict, Any, Optional

from app.service.capterra_loader import _load_all_capterra_reviews_driverless
from app.service.capterra_parser import _parse_capterra_html_for_reviews
# CapterraScrapeResultOutput is implicitly returned via model_dump, not directly instantiated here

async def scrape_capterra_async(
    product_url_str: str,
    start_date_filter_dt: Optional[datetime.date] = None, # Kept for future use
    end_date_filter_dt: Optional[datetime.date] = None    # Kept for future use
) -> Dict[str, Any]:
    overall_start_time = time.perf_counter()
    
    parsed_url = urlparse(product_url_str)
    path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
    company_slug = "unknown-slug"
    if len(path_segments) >= 3 and path_segments[0].lower() == "p": # e.g., /p/PRODUCT_ID/COMPANY_SLUG/reviews
        company_slug = path_segments[1] # Assuming PRODUCT_ID is at index 1 now for structure like /p/ID/SLUG/
        if len(path_segments) > 2 and path_segments[2].lower() != "reviews": # if path is /p/ID/SLUG/something_else - this needs review based on actual Capterra URL structure
             company_slug = path_segments[2] # old logic: path_segments[0].lower() == "p" and path_segments[2]
        # A more robust slug extraction might be needed depending on URL variations.
        # Example: https://www.capterra.com/p/12345/MySoftware/
        # Example: https://www.capterra.com/p/12345/MySoftware/reviews/
        # Let's assume the last segment before /reviews or the last segment if /reviews is not present in /p/ structure
        if path_segments[0].lower() == "p":
            if "reviews" in path_segments:
                reviews_idx = path_segments.index("reviews")
                if reviews_idx > 1: # Ensure there's a segment before 'reviews' and after 'p'
                    company_slug = path_segments[reviews_idx-1]
            elif len(path_segments) > 1: # e.g. /p/ProductID/CompanyName
                 company_slug = path_segments[-1]


    page_source, product_name_selenium_guess = await _load_all_capterra_reviews_driverless(product_url_str, company_slug)

    if not page_source:
        duration = round(time.perf_counter() - overall_start_time, 2)
        return {
            "status": "error",
            "message": "Failed to load page content with Async Driverless.",
            "data": None,
            "summary": {
                "product_url": product_url_str,
                "product_name_guess": product_name_selenium_guess,
                "duration_seconds": duration,
                "total_reviews_scraped": 0
            }
        }

    thread_name_parse = f"CapterraParse-{company_slug[:10]}"
    parsed_data_obj = _parse_capterra_html_for_reviews(
        page_source,
        product_url_str,
        product_name_selenium_guess,
        thread_name_parse
    )
    
    parsed_data_obj.scrape_duration_seconds = round(time.perf_counter() - overall_start_time, 2)

    # TODO: Implement date filtering here if start_date_filter_dt or end_date_filter_dt are provided
    # This would involve iterating through parsed_data_obj.reviews and filtering them.
    # For now, date filters from the request are not applied to the results.

    return {
        "status": "success" if parsed_data_obj.reviews_count_scraped > 0 else "no_reviews_found",
        "data": parsed_data_obj.model_dump(mode='json', by_alias=True), # by_alias=True for HttpUrl
        "summary": {
            "product_url": product_url_str, # Added for consistency
            "product_name": parsed_data_obj.product_name_scraped,
            "total_reviews_scraped": parsed_data_obj.reviews_count_scraped,
            "duration_seconds": parsed_data_obj.scrape_duration_seconds
        }
    }
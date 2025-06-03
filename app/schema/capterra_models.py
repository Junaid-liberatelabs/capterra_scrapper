from typing import List, Optional
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
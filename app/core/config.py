import warnings
import random

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

try:
    import lxml
    DEFAULT_HTML_PARSER = "lxml"
    print("INFO: Using lxml for HTML parsing.")
except ImportError:
    print("Warning: lxml not installed, using html.parser.")
    DEFAULT_HTML_PARSER = "html.parser"

# --- Application Settings ---
APP_VERSION = "1.6.0"
APP_TITLE = "Capterra Scraper API - Refactored"
APP_DESCRIPTION = "Selenium-Driverless with JS click retries, cool downs, and adaptive sleeps. Refactored structure."

# --- Constants for Capterra Selenium Interaction ---
SELENIUM_PAGE_TIMEOUT_S = 40
SELENIUM_ELEMENT_FIND_TIMEOUT_S = 10
SELENIUM_POPUP_FIND_TIMEOUT_S = 5
SELENIUM_INTERACTION_TIMEOUT_S = 7 # General interaction timeout
IFRAME_CONTENT_DOC_TIMEOUT_S = 6.0 # If iframes were to be handled

INITIAL_PAGE_LOAD_SLEEP_S_MIN = 0.5
INITIAL_PAGE_LOAD_SLEEP_S_MAX = 1.0

LOADING_SPINNER_SELECTOR = 'svg[class*="s1xr3lbz"]'
AFTER_SHOW_MORE_CLICK_LOADING_TIMEOUT_S = 28

SHOW_MORE_REVIEWS_BUTTON_SELECTOR = 'button[data-testid="show-more-reviews"]'
REVIEW_CARDS_CONTAINER_SELECTOR = 'div[data-test-id="review-cards-container"]'
INDIVIDUAL_REVIEW_CARD_SELECTOR = 'div.e1xzmg0z.c1ofrhif.typo-10'
OVERLAY_POPUP_CLOSE_BUTTON_SELECTOR = 'div.sb.bkg-light.card.padding-medium i[data-modal-role="close-button"].icon-font-x'

REVIEWS_BATCH_SIZE = 25

# --- Constants for Capterra BS4 Parsing ---
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
import asyncio
import random
import time
import traceback
import selenium_driverless.webdriver as driverless_webdriver
from selenium_driverless.types.by import By
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException,
    StaleElementReferenceException, ElementNotInteractableException, JavascriptException
)
from app.core import config # Ensure config is imported

async def prepare_driverless_driver() -> driverless_webdriver.Chrome:
    options = driverless_webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")  # Run in headless mode
    try:
        task_name = asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'
        print(f"    [{task_name}] Initializing Chrome with options...")
        driver = await driverless_webdriver.Chrome(options=options)
        print(f"    [{task_name}] Chrome initialized. Applying basic stealth script.")
        await driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })
        print(f"    [{task_name}] Stealth script applied.")
        return driver
    except Exception as e:
        task_name = asyncio.current_task().get_name() if asyncio.current_task() else 'DriverSetup'
        print(f"  [{task_name}] CRITICAL DRIVER SETUP ERROR: {type(e).__name__} - {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup selenium-driverless driver: {e}")

async def async_is_element_displayed_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element:
        return False
    try:
        return await driver.execute_script(
            """
            const elem = arguments[0]; 
            if (!elem || !elem.getClientRects || !elem.getClientRects().length) return false; 
            const style = window.getComputedStyle(elem); 
            if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) < 0.1) return false;
            return true;
            """, element
        )
    except (JavascriptException, StaleElementReferenceException):
        return False
    except Exception:
        return False


async def async_is_element_present_in_dom_js(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement) -> bool:
    if not element:
        return False
    try:
        return await driver.execute_script("return arguments[0].isConnected;", element)
    except (JavascriptException, StaleElementReferenceException):
        return False
    except Exception:
        return False

async def try_click_element(driver: driverless_webdriver.Chrome, element: driverless_webdriver.WebElement, thread_name: str = "DefaultThread", always_js_click: bool = False) -> bool:
    try:
        if not element:
            print(f"    [{thread_name}][try_click_element] Element is None.")
            return False
        # Add a check if element is stale before attempting to use it further
        try:
            # A simple check like trying to access a property can reveal staleness
            _ = await element.tag_name
        except StaleElementReferenceException:
            print(f"    [{thread_name}][try_click_element] Element became stale before any action.")
            return False


        if not await async_is_element_present_in_dom_js(driver, element): # Check DOM presence again after potential staleness
            print(f"    [{thread_name}][try_click_element] Element no longer in DOM before attempting click.")
            return False
        try:
            await driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", element)
            await asyncio.sleep(0.15)
        except Exception as e_scroll:
            print(f"    [{thread_name}][try_click_element] Error during scrollIntoView: {type(e_scroll).__name__}. Proceeding with click attempt.")

        script_execution_timeout = config.SELENIUM_INTERACTION_TIMEOUT_S # Use configured timeout

        if always_js_click:
            for attempt in range(3):
                try:
                    if attempt > 0:
                        print(f"    [{thread_name}][try_click_element] JS Click Attempt {attempt + 1}. Cool down before click.")
                        # Ensure element is still valid before re-attempting JS click
                        try:
                            _ = await element.tag_name
                            if not await async_is_element_present_in_dom_js(driver, element):
                                print(f"    [{thread_name}][try_click_element] Element not in DOM for JS Click Attempt {attempt + 1}.")
                                return False
                        except StaleElementReferenceException:
                            print(f"    [{thread_name}][try_click_element] Element stale for JS Click Attempt {attempt + 1}.")
                            return False

                        await driver.execute_script("window.scrollBy(0, 1); window.scrollBy(0, -1);")
                        await asyncio.sleep(random.uniform(0.5, 1.0 + attempt * 0.5))
                    # Pass the timeout to execute_script
                    await driver.execute_script("arguments[0].click();", element, timeout=script_execution_timeout)
                    return True
                except asyncio.TimeoutError: # This can be raised by execute_script if its own timeout is hit
                    print(f"    [{thread_name}][try_click_element] JS click attempt {attempt + 1} hit asyncio.TimeoutError (likely from execute_script internal timeout).")
                    # The log will show the more specific CDP error if that's the cause.
                except JavascriptException as e_js_timeout: # Catch JavascriptException which includes CDP errors
                    # Check if the error message indicates a timeout from CDP
                    if "within 2 seconds" in str(e_js_timeout) or "timeout" in str(e_js_timeout).lower(): # Check for explicit timeout error
                        print(f"    [{thread_name}][try_click_element] JS click attempt {attempt + 1} timed out (JavascriptException: {str(e_js_timeout)[:100]}...).")
                    else:
                        print(f"    [{thread_name}][try_click_element] JS click attempt {attempt + 1} failed with JavascriptException: {type(e_js_timeout).__name__} - {str(e_js_timeout)[:100]}...")
                    # No need to return False immediately if it's a timeout, let the loop retry
                    if attempt == 2: # Last attempt
                         print(f"    [{thread_name}][try_click_element] JS click failed after multiple timeout attempts.")
                         return False
                except StaleElementReferenceException:
                    print(f"    [{thread_name}][try_click_element] Element became stale during JS click attempt {attempt + 1}.")
                    return False # Element is gone, no point retrying this instance
                except Exception as e_js_other:
                    print(f"    [{thread_name}][try_click_element] JS click attempt {attempt + 1} failed with other error: {type(e_js_other).__name__} - {e_js_other}")
                    return False # Unknown error, stop
            return False # Should be unreachable if last attempt leads to return

        # Standard click attempt (if not always_js_click)
        try:
            await element.click(move_to=True) # This click also has its own timeout logic
            return True
        except (ElementClickInterceptedException, ElementNotInteractableException, StaleElementReferenceException, IndexError) as e_click_direct:
            print(f"    [{thread_name}][try_click_element] Direct click failed ({type(e_click_direct).__name__}). Falling back to JS click.")
        except Exception as e_other_direct: # Catch any other Selenium exception
            print(f"    [{thread_name}][try_click_element] Unexpected error during direct click: {type(e_other_direct).__name__}. Falling back to JS click.")

        await asyncio.sleep(0.05)
        
        # Re-check element validity before JS fallback
        try:
            _ = await element.tag_name
            if not await async_is_element_present_in_dom_js(driver, element):
                print(f"    [{thread_name}][try_click_element] Element no longer in DOM before JS fallback click.")
                return False
        except StaleElementReferenceException:
            print(f"    [{thread_name}][try_click_element] Element stale before JS fallback click.")
            return False

        for attempt in range(2): # JS Fallback click with retry
            try:
                if attempt > 0:
                    print(f"    [{thread_name}][try_click_element] JS Fallback Click Attempt {attempt + 1}. Cool down.")
                    await driver.execute_script("window.scrollBy(0, 1); window.scrollBy(0, -1);")
                    await asyncio.sleep(random.uniform(0.5, 1.0 + attempt * 0.5))
                # Pass the timeout to execute_script
                await driver.execute_script("arguments[0].click();", element, timeout=script_execution_timeout)
                return True
            except asyncio.TimeoutError:
                 print(f"    [{thread_name}][try_click_element] JS fallback click attempt {attempt + 1} hit asyncio.TimeoutError.")
            except JavascriptException as e_js_fb_timeout:
                if "within 2 seconds" in str(e_js_fb_timeout) or "timeout" in str(e_js_fb_timeout).lower():
                    print(f"    [{thread_name}][try_click_element] JS fallback click attempt {attempt + 1} timed out (JavascriptException: {str(e_js_fb_timeout)[:100]}...).")
                else:
                    print(f"    [{thread_name}][try_click_element] JS fallback click attempt {attempt + 1} failed with JavascriptException: {type(e_js_fb_timeout).__name__} - {str(e_js_fb_timeout)[:100]}...")
                if attempt == 1:
                     print(f"    [{thread_name}][try_click_element] JS fallback click failed after multiple timeout attempts.")
                     return False
            except StaleElementReferenceException:
                print(f"    [{thread_name}][try_click_element] Element became stale during JS fallback click attempt {attempt + 1}.")
                return False
            except Exception as e_js_other_fb:
                print(f"    [{thread_name}][try_click_element] JS fallback click attempt {attempt + 1} failed with other error: {type(e_js_other_fb).__name__} - {e_js_other_fb}")
                return False
        return False
    except StaleElementReferenceException: # Catch staleness at the very beginning if initial checks pass but element goes stale.
        print(f"    [{thread_name}][try_click_element] Element became stale in outer scope.")
        return False
    except Exception as e_overall:
        print(f"    [{thread_name}][try_click_element] Outer scope unexpected error: {type(e_overall).__name__} - {e_overall}")
        return False

async def handle_specific_overlay_popup(driver: driverless_webdriver.Chrome, thread_name: str) -> bool:
    try:
        close_buttons = await driver.find_elements(By.CSS_SELECTOR, config.OVERLAY_POPUP_CLOSE_BUTTON_SELECTOR, timeout=config.SELENIUM_POPUP_FIND_TIMEOUT_S / 2)
        if close_buttons:
            for btn in close_buttons:
                if await async_is_element_displayed_js(driver, btn):
                    if await try_click_element(driver, btn, thread_name=f"{thread_name}-OverlayPopup", always_js_click=True):
                        print(f"      [{thread_name}] Clicked specific overlay popup close button.")
                        await asyncio.sleep(0.7)
                        return True
    except TimeoutException:
        pass
    except Exception as e:
        print(f"      [{thread_name}] Error handling specific overlay popup: {type(e).__name__} - {e}")
    return False

async def async_wait_until_reviews_load(driver: driverless_webdriver.Chrome, initial_review_count: int, review_card_selector: str, loader_selector: str, timeout: float, thread_name: str) -> bool:
    start_time = time.monotonic()
    loader_was_seen_and_disappeared_without_increase = False
    while time.monotonic() - start_time < timeout:
        current_reviews_elements = []
        try:
            current_reviews_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / 4)
            if len(current_reviews_elements) > initial_review_count:
                return True
        except TimeoutException:
            pass
        except Exception as e_find:
            print(f"      [{thread_name}] Error finding reviews in wait: {type(e_find).__name__}")
            await asyncio.sleep(0.2)
            continue
        try:
            loader_elements = await driver.find_elements(By.CSS_SELECTOR, loader_selector, timeout=0.25)
            is_loader_currently_displayed = any(await async_is_element_displayed_js(driver, el) for el in loader_elements)
            if loader_was_seen_and_disappeared_without_increase and not is_loader_currently_displayed:
                # If loader was seen, disappeared, and review count hasn't increased, assume loading finished or failed.
                return False 
            if is_loader_currently_displayed:
                loader_was_seen_and_disappeared_without_increase = True 
        except Exception:
            pass # Ignore errors in loader detection, primary check is review count
        await asyncio.sleep(0.35)
    
    # Final check after timeout
    final_review_elements = []
    try:
        final_review_elements = await driver.find_elements(By.CSS_SELECTOR, review_card_selector, timeout=config.SELENIUM_ELEMENT_FIND_TIMEOUT_S / 2)
    except Exception:
        pass
    return len(final_review_elements) > initial_review_count
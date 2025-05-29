from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def get_html(url):
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        # Navigate to the URL
        driver.get(url)
        # Get the page source (HTML)
        html = driver.page_source
        return html
    finally:
        driver.quit()
if __name__ == "__main__":
    url = "https://www.capterra.com/p/135005/Scholar-LMS/reviews/"
    html_content = get_html(url)
    with open("capterra_scholar_lms.html", "w", encoding="utf-8") as file:
        file.write(html_content)    
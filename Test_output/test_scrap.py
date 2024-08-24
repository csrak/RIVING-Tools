import os
import time
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Function to install GeckoDriver with a cache check
def install_firefox_driver():
    timestamp_file = 'firefox_driver_last_install_time.txt'
    install_interval = 7 * 24 * 60 * 60  # 7 days in seconds

    if os.path.exists(timestamp_file):
        with open(timestamp_file, 'r') as f:
            last_install_time = float(f.read())
    else:
        last_install_time = 0

    current_time = time.time()

    if current_time - last_install_time > install_interval:
        driver_path = GeckoDriverManager().install()
        with open(timestamp_file, 'w') as f:
            f.write(str(current_time))
    else:
        driver_path = GeckoDriverManager().install()  # use the cached version

    return driver_path

# Set up Firefox options
firefox_options = Options()
firefox_options.add_argument('--headless')
firefox_options.add_argument('--log-level=3')

# Install the Firefox driver using the function
driver_path = install_firefox_driver()

# Initialize the WebDriver for Firefox
driver_fire = webdriver.Firefox(service=FirefoxService(driver_path), options=firefox_options)

# Define the URL for the specific stock
stock_url = "https://www.bloomberg.com/quote/ANDINAB:CI"  # Example URL for Embotelladora Andina

# Open the URL with Selenium
driver_fire.get(stock_url)

# Wait for the JavaScript to load and find the element that contains the stock value
wait = WebDriverWait(driver_fire, 1)
price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sized-price")))

# Extract the text value
stock_price = price_element.text

# Print the stock value
print(f"The current stock price is {stock_price} CLP")

# Close the browser
driver_fire.quit()

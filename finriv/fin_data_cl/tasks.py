# import logging
# import random
# import time
# import math
# from selenium import webdriver
# from selenium.webdriver.firefox.service import Service as FirefoxService
# from selenium.webdriver.firefox.options import Options
# from webdriver_manager.firefox import GeckoDriverManager
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from datetime import date
# from decimal import Decimal
# from fake_useragent import UserAgent  # For rotating user agents
# from .models import ProcessedData, FinancialRatio, Ticker
# from tqdm import tqdm
#
# logger = logging.getLogger('fin_data_cl')
#
# def get_random_user_agent():
#     ua = UserAgent()
#     return ua.random
#
# def setup_driver():
#     firefox_options = Options()
#     firefox_options.add_argument('--headless')
#     firefox_options.add_argument('--log-level=3')
#
#     user_agent = get_random_user_agent()
#     firefox_options.add_argument(f'user-agent={user_agent}')
#
#     driver_path = GeckoDriverManager().install()
#     driver = webdriver.Firefox(service=FirefoxService(driver_path), options=firefox_options)
#
#     logger.debug(f"Using User-Agent: {user_agent}")
#
#     return driver
#
# def extract_tickers():
#     tickers = Ticker.objects.values_list('symbol', flat=True).distinct()
#     logger.debug(f"Extracted tickers: {tickers}")
#     return tickers
#
# def scrape_marketwatch_price(ticker, driver):
#     try:
#         formatted_ticker = ticker.replace('-', '.')
#
#         marketwatch_url = f"https://www.marketwatch.com/investing/stock/{formatted_ticker}?countrycode=cl"
#         logger.debug(f"Attempting to scrape MarketWatch for {ticker} at {marketwatch_url}")
#
#         driver.get(marketwatch_url)
#         time.sleep(random.uniform(2, 5))  # Random delay to mimic human behavior
#
#         wait = WebDriverWait(driver, 10)
#         price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "bg-quote.value")))
#
#         stock_price = price_element.text.replace(',', '')
#         stock_price = float(stock_price)
#
#         logger.debug(f"Scraped price from MarketWatch for {ticker}: {stock_price}")
#         return stock_price
#
#     except Exception as e:
#         logger.error(f"Failed to scrape MarketWatch for {ticker}: {e}")
#         return None
#
# def update_stock_prices():
#     tickers = extract_tickers()
#
#     try:
#         for ticker_symbol in tickers:
#             driver_fire = setup_driver()
#
#             try:
#                 formatted_ticker = ticker_symbol.replace('-', '') + ':CI'
#                 bloomberg_url = f"https://www.bloomberg.com/quote/{formatted_ticker}"
#
#                 logger.debug(f"Scraping Bloomberg for {ticker_symbol} at {bloomberg_url}")
#
#                 driver_fire.get(bloomberg_url)
#                 time.sleep(random.uniform(2, 5))  # Random delay
#
#                 wait = WebDriverWait(driver_fire, 10)
#                 try:
#                     price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sized-price")))
#
#                     raw_price_text = price_element.text
#                     logger.debug(f"Raw price text from Bloomberg for {ticker_symbol}: {raw_price_text}")
#
#                     stock_price = float(raw_price_text.replace(',', '')) if raw_price_text else None
#
#                     if stock_price is None or math.isnan(stock_price):
#                         logger.warning(f"Scraped price is NaN for {ticker_symbol}. Trying MarketWatch...")
#                         stock_price = scrape_marketwatch_price(ticker_symbol, driver_fire)
#
#                     logger.debug(f"Final price for {ticker_symbol}: {stock_price}")
#
#                 except Exception as e:
#                     logger.warning(f"Bloomberg failed for {ticker_symbol}: {e}. Trying MarketWatch...")
#                     stock_price = scrape_marketwatch_price(ticker_symbol, driver_fire)
#
#                 if stock_price is not None and not math.isnan(stock_price):
#                     ticker_instance = Ticker.objects.get(symbol=ticker_symbol)
#                     UpdatedPrice.objects.update_or_create(
#                         ticker=ticker_instance,
#                         date=date.today(),
#                         defaults={'price': Decimal(stock_price)}
#                     )
#                     logger.debug(f"Saved price for {ticker_symbol} on {date.today()}: {stock_price}")
#                 else:
#                     logger.error(f"Failed to retrieve valid price for {ticker_symbol} from both Bloomberg and MarketWatch.")
#
#             finally:
#                 driver_fire.quit()
#                 logger.debug("Closed the browser")
#
#     finally:
#         logger.debug("Completed all tickers")
#
# def calculate_ratios():
#     ratios = [
#         ('assets_liabilities', lambda data: data.assets / data.liabilities if data.liabilities else None),
#         ('profit_assets', lambda data: data.net_profit / data.assets if data.assets else None),
#     ]
#
#     tickers = Ticker.objects.all()
#     dates = ProcessedData.objects.values_list('date', flat=True).distinct()
#
#     total_steps = len(tickers) * len(dates)
#     progress_bar = tqdm(total=total_steps, desc="Calculating Ratios")
#
#     for ticker_instance in tickers:
#         for date_instance in dates:
#             data = ProcessedData.objects.filter(ticker=ticker_instance, date=date_instance).first()
#             if data:
#                 for ratio_name, ratio_calculation in ratios:
#                     ratio_value = ratio_calculation(data)
#                     if ratio_value is not None:
#                         FinancialRatio.objects.update_or_create(
#                             ticker=ticker_instance,
#                             date=date_instance,
#                             ratio_name=ratio_name,
#                             defaults={'ratio_value': Decimal(ratio_value)},
#                         )
#             progress_bar.update(1)
#
#     progress_bar.close()
#
# def update_prices_and_calculate_ratios():
#     update_stock_prices()
#     calculate_ratios()

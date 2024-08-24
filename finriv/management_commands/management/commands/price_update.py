import time
import pandas as pd
from tqdm import tqdm
from django.core.management.base import BaseCommand
from fin_data_cl.models import PriceData, FinancialData
import datetime
from decimal import Decimal, InvalidOperation
import yfinance as yf  # Yahoo Finance API


# Function to ensure values are compatible with Django DecimalField
def to_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


# Function to get the latest available price and market cap using Yahoo Finance API
def yahoo_quote_cl(ticker):
    # Append .SN for Chilean stocks
    ticker_sn = ticker + '.SN'

    try:
        # Fetch the data using yfinance
        stock = yf.Ticker(ticker_sn)
        hist = stock.history(period="1mo")  # Get the last 1 month to ensure we capture the last transaction

        if hist.empty:
            print(
                f"Warning: No data returned for ticker {ticker_sn}. It may not be transacted anymore or not available on Yahoo Finance.")
            return None, None

        # Get the last available closing price
        stock_price = hist['Close'].dropna().iloc[-1]  # Get the last non-NaN close price

        # Fetch the market cap from the stock info
        stock_info = stock.info
        market_cap = stock_info.get('marketCap', None)

        if stock_price is not None:
            stock_price = float(stock_price)

        if market_cap is not None:
            market_cap = float(market_cap)

        return to_decimal(stock_price), to_decimal(market_cap)

    except Exception as e:
        print(f"Error fetching data for ticker {ticker_sn}: {str(e)}")
        return None, None


class Command(BaseCommand):
    help = 'Populate prices and market caps for each ticker using the Yahoo Finance API'

    def handle(self, *args, **kwargs):
        # Get a list of unique tickers from FinancialData
        tickers = FinancialData.objects.values_list('ticker', flat=True).distinct()

        # Initialize progress bar
        for ticker in tqdm(tickers, desc="Fetching Prices and Market Caps"):
            try:
                # Fetch the latest price and market cap using Yahoo Finance API
                price, market_cap = yahoo_quote_cl(ticker)

                # Debug print to see what's fetched
                print(f"Ticker: {ticker}, Price: {price}, Market Cap: {market_cap}")

                # If the price and market cap are valid, save them to the PriceData model
                if price is not None and market_cap is not None:
                    price_data = PriceData(
                        ticker=ticker,
                        date=datetime.date.today(),
                        price=price,
                        market_cap=market_cap
                    )
                    price_data.save()

                    print(f"Saved price and market cap for {ticker}: {price}, {market_cap} on {datetime.date.today()}")

            except Exception as e:
                print(f"Error fetching or saving price and market cap for {ticker}: {str(e)}")

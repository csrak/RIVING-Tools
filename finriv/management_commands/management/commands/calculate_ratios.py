import math
from django.core.management.base import BaseCommand
from fin_data_cl.models import FinancialData, FinancialRatio, PriceData, DividendSummary
from decimal import Decimal


def get_latest_dividend_total(ticker):
    """
    Retrieves the total dividends for the latest year for a given ticker using the latest() method.

    Args:
        ticker (str): The ticker symbol of the company.

    Returns:
        dict: A dictionary containing 'ticker', 'year', 'total_dividends', and 'dividend_count'.
              Returns None if no records are found for the ticker.
    """
    latest_dividend = DividendSummary.objects.filter(ticker=ticker.upper()).latest()

    if latest_dividend:
        return {
            'ticker': latest_dividend.ticker,
            'year': latest_dividend.year,
            'total_dividends': latest_dividend.total_dividends,
            'dividend_count': latest_dividend.dividend_count
        }
    else:
        print(f"Dividends not found for {ticker}")
        return None
def calculate_ratios(ticker, date):
    try:

        # Get the latest available date for balance sheet data
        latest_financial_data = FinancialData.objects.filter(ticker=ticker, date=date).first()
        latest_divs = get_latest_dividend_total(ticker)['total_dividends']
        before_divs =  DividendSummary.objects.filter(ticker=ticker.upper()).order_by('-year')[:2]
        if len(before_divs) >= 2:
            before_divs = before_divs[1].total_dividends
        # Fetch the last four quarters for income statement data to annualize
        last_4_quarters = FinancialData.objects.filter(ticker=ticker).order_by('-date')[:4]

        # If we have fewer than 4 quarters, we can't annualize properly
        if len(last_4_quarters) < 4:
            return None

        # Aggregate data for the last 4 quarters
        aggregated_data = {
            'revenue': sum([d.revenue for d in last_4_quarters if d.revenue is not None]),
            'net_profit': sum([d.net_profit for d in last_4_quarters if d.net_profit is not None]),
            'operating_profit': sum([d.operating_profit for d in last_4_quarters if d.operating_profit is not None]),
            'eps': sum([d.eps for d in last_4_quarters if d.eps is not None]),
            'cost_of_sales': sum([d.cost_of_sales for d in last_4_quarters if d.cost_of_sales is not None]),
            'ebit': sum([d.ebit for d in last_4_quarters if d.ebit is not None]),
            'depreciation': sum([d.depreciation for d in last_4_quarters if d.depreciation is not None]),
            'interest': sum([d.interest for d in last_4_quarters if d.interest is not None]),
        }

        # Use the latest balance sheet data for these metrics
        balance_sheet_data = {
            'equity': latest_financial_data.equity,
            'assets': latest_financial_data.assets,
            'liabilities': latest_financial_data.liabilities,
            'current_assets': latest_financial_data.current_assets,
            'current_liabilities': latest_financial_data.current_liabilities,
            'inventories': latest_financial_data.inventories,
            'cash': latest_financial_data.cash,
            'shares': latest_financial_data.shares,
        }

        # Fetch the most recent price and market cap data
        price_data = PriceData.objects.filter(ticker=ticker).order_by('-date').first()
        peg_ratio = 0

        if not price_data or not latest_financial_data:
            return None  # Skip if data is missing

        # Calculate ratios with robust handling for None and zero values price_data.market_cap
        if aggregated_data['eps']:
            pe_ratio = to_decimal(price_data.price) / to_decimal(aggregated_data['eps'])
        elif price_data.market_cap and aggregated_data['net_profit']:
            pe_ratio = to_decimal(price_data.market_cap) / to_decimal(aggregated_data['net_profit'])
        else:
            pe_ratio = None

        if price_data.market_cap:
            pb_ratio = to_decimal(price_data.market_cap) / to_decimal(balance_sheet_data['equity'] ) if balance_sheet_data['equity']  else None
            ps_ratio = to_decimal(price_data.market_cap) / to_decimal(aggregated_data['revenue'] ) if aggregated_data['revenue'] else None
        elif balance_sheet_data['shares']:
            pb_ratio = to_decimal(price_data.price) / to_decimal(balance_sheet_data['equity'] / balance_sheet_data['shares']) if balance_sheet_data['equity'] else None
            ps_ratio = to_decimal(price_data.price) / to_decimal(aggregated_data['revenue'] / balance_sheet_data['shares']) if aggregated_data['revenue']  else None

        # Enterprise Value calculation using market cap if available
        enterprise_value = to_decimal(price_data.market_cap) + to_decimal(balance_sheet_data['liabilities']) - to_decimal(balance_sheet_data['cash']) if price_data.market_cap else None
        ev_ebit = to_decimal(enterprise_value) / to_decimal(aggregated_data['ebit']) if enterprise_value and aggregated_data['ebit'] else None

        gross_profit_margin = to_decimal((aggregated_data['revenue'] - aggregated_data['cost_of_sales']) / aggregated_data['revenue']) if aggregated_data['revenue'] else None
        operating_profit_margin = to_decimal(aggregated_data['operating_profit'] / aggregated_data['revenue']) if aggregated_data['revenue'] else None
        net_profit_margin = to_decimal(aggregated_data['net_profit'] / aggregated_data['revenue']) if aggregated_data['revenue'] else None
        return_on_assets = to_decimal(aggregated_data['net_profit'] / balance_sheet_data['assets']) if balance_sheet_data['assets'] else None
        return_on_equity = to_decimal(aggregated_data['net_profit'] / balance_sheet_data['equity']) if balance_sheet_data['equity'] else None
        debt_to_equity = to_decimal(balance_sheet_data['liabilities'] / balance_sheet_data['equity']) if balance_sheet_data['equity'] else None
        current_ratio = to_decimal(balance_sheet_data['current_assets'] / balance_sheet_data['current_liabilities']) if balance_sheet_data['current_liabilities'] else None
        quick_ratio = to_decimal((balance_sheet_data['current_assets'] - balance_sheet_data['inventories']) / balance_sheet_data['current_liabilities']) if balance_sheet_data['current_liabilities'] else None
        dividend_yield = to_decimal(latest_divs / price_data.price)  if latest_divs else None
        before_dividend_yield = to_decimal(before_divs / price_data.price)  if latest_divs else None
        # Create or update the financial ratio record
        financial_ratio, created = FinancialRatio.objects.update_or_create(
            ticker=ticker,
            date=date,
            defaults={
                'pe_ratio': pe_ratio,
                'pb_ratio': pb_ratio,
                'ps_ratio': ps_ratio,
                'peg_ratio': peg_ratio,
                'ev_ebitda': ev_ebit,
                'gross_profit_margin': gross_profit_margin,
                'operating_profit_margin': operating_profit_margin,
                'net_profit_margin': net_profit_margin,
                'return_on_assets': return_on_assets,
                'return_on_equity': return_on_equity,
                'debt_to_equity': debt_to_equity,
                'current_ratio': current_ratio,
                'quick_ratio': quick_ratio,
                'dividend_yield':dividend_yield,
                'before_dividend_yield':before_dividend_yield
            }
        )

    except FinancialData.DoesNotExist:
        print(f"Financial data for ticker {ticker} on {date} not found.")
        return None
    except Exception as e:
        print(f"Error calculating ratios for ticker {ticker} on {date}: {str(e)}")
        return None

def to_decimal(value):
    try:
        return Decimal(str(value)) if value is not None and not math.isnan(value) else None
    except (InvalidOperation, ValueError):
        return None

class Command(BaseCommand):
    help = 'Calculate and store financial ratios for all tickers'

    def handle(self, *args, **kwargs):
        # Get unique tickers and dates from FinancialData
        tickers = FinancialData.objects.values_list('ticker', flat=True).distinct()

        for ticker in tickers:
            dates = FinancialData.objects.filter(ticker=ticker).values_list('date', flat=True).distinct()
            for date in dates:
                calculate_ratios(ticker, date)
                print(f"Ratios calculated for {ticker} on {date}.")

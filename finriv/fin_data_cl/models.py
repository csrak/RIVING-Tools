from django.db import models
from decimal import Decimal

class TickerData(models.Model):
    ticker = models.CharField(max_length=10)

    class Meta:
        abstract = True

class FinancialData(TickerData):
    date = models.DateField()
    revenue = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    net_profit = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    operating_profit = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    non_controlling_profit = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    eps = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    operating_eps = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    interest_revenue = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_from_sales = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_from_yield = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_from_rent = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_to_payments = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_to_other_payments = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    speculation_cash = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    current_payables = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cost_of_sales = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    ebit = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    depreciation = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    interest = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    current_assets = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    liabilities = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    marketable_securities = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    current_other_assets = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    provisions_for_employees = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    non_current_assets = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    goodwill = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    intangible_assets = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    assets = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    current_liabilities = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    equity = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    shares = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    inventories = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    shares_authorized = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    net_operating_cashflows = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    net_investing_cashflows = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    net_financing_cashflows = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    payment_for_supplies = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    payment_to_employees = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    dividends_paid = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    forex = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    trade_receivables = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    prepayments = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_on_hands = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_on_banks = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    cash_short_investment = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    employee_benefits = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)

    class Meta:
        unique_together = ('ticker', 'date')
        verbose_name = 'Financial Data'
        verbose_name_plural = 'Financial Data'

from django.db import models

class PriceData(models.Model):
    ticker = models.CharField(max_length=10)
    date = models.DateField()
    price = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    market_cap = models.DecimalField(max_digits=30, decimal_places=2, null=True, blank=True)  # New field for Market Cap

    def __str__(self):
        return f"{self.ticker} - {self.date}"

class FinancialRatio(models.Model):
    ticker = models.CharField(max_length=10)
    date = models.DateField()

    pe_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Price-to-Earnings
    pb_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Price-to-Book
    ps_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Price-to-Sales
    peg_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Price-to-Earnings Growth
    ev_ebitda = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # EV/EBITDA
    ev_sales = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # EV/Sales
    gross_profit_margin = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Gross Profit Margin
    operating_profit_margin = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Operating Profit Margin
    net_profit_margin = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Net Profit Margin
    return_on_assets = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Return on Assets
    return_on_equity = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Return on Equity
    return_on_investment = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Return on Investment
    debt_to_equity = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Debt-to-Equity
    current_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Current Ratio
    quick_ratio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Quick Ratio

    class Meta:
        unique_together = ('ticker', 'date')
        verbose_name = 'Financial Ratio'
        verbose_name_plural = 'Financial Ratios'

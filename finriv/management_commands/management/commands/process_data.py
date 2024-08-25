import pandas as pd
from decimal import Decimal, InvalidOperation
from tqdm import tqdm
from django.core.management.base import BaseCommand
from fin_data_cl.models import FinancialData, PriceData, FinancialRatio

class Command(BaseCommand):
    help = 'Import financial data from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r'C:\Users\s0Csrak\OneDrive\Documents\python\RIVING-Tools\Data\Chile\Database_in_CLP.csv'

        def clean_column_names(df):
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('-', '_')
                .str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
            )
            return df

        def parse_and_create_database(file_path):
            df = pd.read_csv(file_path)
            df = clean_column_names(df)

            metrics = [
                'revenue', 'net_profit', 'operating_profit', 'non_controlling_profit', 'eps', 'operating_eps',
                'interest_revenue', 'cash_from_sales', 'cash_from_yield', 'cash_from_rent', 'cash_to_payments',
                'cash_to_other_payments', 'speculation_cash', 'current_payables', 'cost_of_sales', 'ebit',
                'depreciation', 'interest', 'cash', 'current_assets', 'liabilities', 'marketable_securities',
                'current_other_assets', 'provisions_for_employees', 'non_current_assets', 'goodwill',
                'intangible_assets', 'assets', 'current_liabilities', 'equity', 'shares', 'inventories',
                'shares_authorized', 'net_operating_cashflows', 'net_investing_cashflows', 'net_financing_cashflows',
                'payment_for_supplies', 'payment_to_employees', 'dividends_paid', 'forex', 'trade_receivables',
                'prepayments', 'cash_on_hands', 'cash_on_banks', 'cash_short_investment', 'employee_benefits'
            ]

            ticker_tables = {}

            for metric in tqdm(metrics, desc="Processing Metrics"):
                df_relevant = df[['date', 'ticker', metric]].copy()
                df_relevant['code'] = df_relevant[metric].shift(-1).where(df_relevant.index % 2 == 0)
                df_relevant = df_relevant[df_relevant.index % 2 == 0]

                df_relevant['date'] = pd.to_datetime(df_relevant['date'], format='%Y%m')
                df_relevant[metric] = df_relevant[metric].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
                df_relevant['code'] = pd.to_numeric(df_relevant['code'], errors='coerce').replace({pd.NA: None})
                df_relevant = df_relevant.sort_values(by=['ticker', 'date'])

                tickers = df_relevant['ticker'].unique()

                for ticker in tickers:
                    ticker_data = df_relevant[df_relevant['ticker'] == ticker]
                    prev_processed_value = Decimal(0)
                    processed_values = []

                    for _, row in ticker_data.iterrows():
                        unprocessed_value = row[metric]
                        code = row['code']

                        if unprocessed_value is None or code is None:
                            continue  # Skip rows with invalid data

                        quarter = row['date'].month // 3

                        if quarter == 1:
                            prev_processed_value = Decimal(0)

                        try:
                            if code == 10:
                                processed_value = Decimal(unprocessed_value)
                            elif code == 11 or code == 12:
                                processed_value = Decimal(unprocessed_value) - prev_processed_value
                            else:
                                processed_value = Decimal(unprocessed_value)
                        except InvalidOperation:
                            processed_value = None  # Skip invalid Decimal operations

                        if processed_value is not None:
                            prev_processed_value += processed_value
                            processed_values.append((row['date'], processed_value))

                    metric_df = pd.DataFrame(processed_values, columns=['date', metric])

                    if ticker in ticker_tables:
                        ticker_tables[ticker] = pd.merge(ticker_tables[ticker], metric_df, on='date', how='outer')
                    else:
                        ticker_tables[ticker] = metric_df

            tickers_list = list(ticker_tables.keys())
            tables_list = [ticker_tables[ticker] for ticker in tickers_list]

            return tickers_list, tables_list

        def save_financial_data(ticker, date, **kwargs):
            cleaned_data = {k: (v if pd.notna(v) else None) for k, v in kwargs.items()}
            FinancialData.objects.update_or_create(
                ticker=ticker,
                date=date,
                defaults=cleaned_data
            )

        ticker_list, tables_list = parse_and_create_database(file_path)

        for ticker, df in tqdm(zip(ticker_list, tables_list), desc="Saving Data", total=len(ticker_list)):
            for _, row in df.iterrows():
                save_financial_data(
                    ticker=ticker,
                    date=row['date'],
                    **{metric: row.get(metric) for metric in df.columns if metric != 'date'}
                )

import csv
from decimal import Decimal, InvalidOperation
from django.core.management.base import BaseCommand
from fin_data_cl.models import DividendSummary
from django.db.models import Max, F
import os

class Command(BaseCommand):
    help = 'Calculate total dividends and dividend counts per year for each company and generate a report of the latest dividends.'

    def handle(self, *args, **kwargs):
        # Define the input CSV file path
        input_file_path = r'C:\Users\s0Csrak\OneDrive\Documents\python\RIVING-Tools\Data\Chile\Dividends\Dividends__2018_2024.csv'  # Update this to the correct file path

        # Define the output report file path
        output_file_path = r'C:\Users\s0Csrak\OneDrive\Documents\python\RIVING-Tools\Data\Chile\Dividends\Latest_Dividends_Report.txt'  # Update as needed

        # Create a dictionary to hold the aggregated data
        dividend_data = {}

        # Read the CSV file and aggregate data
        self.stdout.write('Reading and aggregating dividend data from CSV...')
        try:
            with open(input_file_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)

                # Normalize column names to lowercase
                reader.fieldnames = [field.lower() for field in reader.fieldnames]

                for row in reader:
                    # Extract and normalize data
                    ticker = row.get('ticker', '').strip().upper()
                    try:
                        year = int(row.get('year', '').strip())
                    except ValueError:
                        self.stderr.write(f"Invalid year value: {row.get('year')}. Skipping row.")
                        continue  # Skip rows where the year is invalid

                    dividends_str = row.get('dividends', '').strip()
                    try:
                        dividends = Decimal(dividends_str)
                    except (InvalidOperation, ValueError):
                        self.stderr.write(f"Invalid dividends value: {dividends_str}. Skipping row.")
                        continue  # Skip rows where the dividends amount is invalid or missing

                    key = (ticker, year)

                    if key not in dividend_data:
                        dividend_data[key] = {'total_dividends': Decimal('0.00'), 'dividend_count': 0}

                    dividend_data[key]['total_dividends'] += dividends
                    dividend_data[key]['dividend_count'] += 1

        except FileNotFoundError:
            self.stderr.write(f"Input file not found: {input_file_path}")
            return
        except Exception as e:
            self.stderr.write(f"An error occurred while reading the CSV file: {e}")
            return

        self.stdout.write('Aggregated dividend data successfully.')

        # Save the aggregated data to the database
        self.stdout.write('Saving aggregated data to the database...')
        for key, data in dividend_data.items():
            ticker, year = key
            DividendSummary.objects.update_or_create(
                ticker=ticker,
                year=year,
                defaults={
                    'total_dividends': data['total_dividends'],
                    'dividend_count': data['dividend_count']
                }
            )
        self.stdout.write(self.style.SUCCESS('Dividend summary calculation complete.'))

        # Retrieve the latest dividends for each ticker
        self.stdout.write('Retrieving the latest dividends for each ticker...')
        latest_dividends = DividendSummary.objects.values('ticker').annotate(
            latest_year=Max('year')
        ).order_by('ticker')

        # Prepare a list of dictionaries containing the latest dividend data
        latest_dividend_data = []
        for entry in latest_dividends:
            ticker = entry['ticker']
            latest_year = entry['latest_year']
            try:
                dividend = DividendSummary.objects.get(ticker=ticker, year=latest_year)
                latest_dividend_data.append({
                    'ticker': ticker,
                    'year': latest_year,
                    'total_dividends': dividend.total_dividends,
                    'dividend_count': dividend.dividend_count
                })
            except DividendSummary.DoesNotExist:
                self.stderr.write(f"No DividendSummary found for ticker {ticker} in year {latest_year}.")

        # Write the latest dividends to the text file
        self.stdout.write(f'Writing the latest dividends to {output_file_path}...')
        try:
            with open(output_file_path, 'w', encoding='utf-8') as report_file:
                report_file.write("Latest Dividends Report\n")
                report_file.write("========================\n\n")
                for data in latest_dividend_data:
                    report_file.write(
                        f"Ticker: {data['ticker']}\n"
                        f"Year: {data['year']}\n"
                        f"Total Dividends: {data['total_dividends']}\n"
                        f"Dividend Count: {data['dividend_count']}\n"
                        "------------------------\n"
                    )
            self.stdout.write(self.style.SUCCESS(f'Latest dividends report successfully written to {output_file_path}.'))
        except IOError as e:
            self.stderr.write(f"Failed to write the report file: {e}")

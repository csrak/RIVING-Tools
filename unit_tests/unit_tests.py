import sys
import os
import unittest
import pandas as pd
import numpy as np
import datetime

# Ensure the main package folder is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import your module
import Find as financial_stuff

class TestFinancialFunctions(unittest.TestCase):

    def setUp(self):
        # Setup some sample data that can be used across multiple tests
        self.df = pd.DataFrame({
            'Ticker': ['AAPL', 'GOOGL', 'MSFT'],
            'Current Liabilities': [1000, 2000, 1500],
            'Current Assets': [3000, 5000, 2500],
            'Net Profit': [500, 1000, 800],
            'Non-Controlling Profit': [50, 100, 80],
            'Some Metric': [1.5, 2.5, 3.5]
        })

    def test_find_condition(self):
        print("\nRunning test_find_condition...")
        condition = 'Some Metric<3.0'
        financial_stuff.find_condition(self.df, condition)
        print("Tested condition:", condition)

    def test_quarters_to_years(self):
        print("\nRunning test_quarters_to_years...")
        data = [1, 2, 3, 4]
        dates = [202001, 202002, 202003, 202004]
        result = financial_stuff.quarters_to_years(data, dates)
        print("Data:", data)
        print("Dates:", dates)
        print("Result:", result)

    def test_price_to_parameter(self):
        print("\nRunning test_price_to_parameter...")
        df = pd.DataFrame({
            'ticker': ['AAPL', 'GOOGL', 'MSFT'],
            'price': [150, 2000, 250],
            'shares': [500, 1000, 800],
            'Net Profit': [500, 1000, 800],
            'Non-Controlling Profit': [50, 100, 80]
        })
        param = 'Net Profit'
        result = financial_stuff.price_to_parameter(df, param, debug=True)
        print("Parameter:", param)
        print("Result:\n", result)

    def test_quick_ratio(self):
        print("\nRunning test_quick_ratio...")
        result = financial_stuff.quick_ratio(self.df)
        print("Quick Ratio Result:\n", result)

    def test_prices_to_file(self):
        print("\nRunning test_prices_to_file...")
        financial_stuff.prices_to_file()
        print("Prices have been written to file.")

    #def test_dividend_yields(self):
    #    print("\nRunning test_dividend_yields...")
    #    dfile = 'mock_dividends_file.csv'
    #    datafolder = 'Data/Chile/'
    #    result = financial_stuff.dividend_yields(dfile=dfile, datafolder=datafolder)
    #    print("Dividend Yields Calculation Complete. Results stored in Prices.csv")

    #def test_create_cleaned_database(self):
    #    print("\nRunning test_create_cleaned_database...")
    #    folder = 'mock_cleaned_database'
    #    financial_stuff.create_cleaned_database(self.df, folder)
    #    print(f"Database cleaned and files saved in {folder}.")

if __name__ == '__main__':
    if not os.path.isdir('mock_data_folder'):
        os.mkdir('mock_data_folder')
    unittest.main()

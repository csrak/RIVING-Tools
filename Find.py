import sys
import os
import time
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from matplotlib import pyplot as plt
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

import live_data as ld
import Requests_CL as rcl
import Search_chile as SC

# Suppress chained assignment warnings from pandas
pd.options.mode.chained_assignment = None
root_dir = Path(__file__).resolve().parent

# Configure Selenium WebDriver for Chrome in headless mode
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--log-level=3')

# Firefox WebDriver setup for use in one of the functions
driver_fire = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))

# Function to find tickers that meet a specific condition
# The condition should be written as "column_name</=/>value"
# Returns tickers that satisfy the condition based on the data in the provided DataFrame
def find_condition(df, condition):
    # Check if the condition contains a less than, greater than, or equal to comparison
    if '<' in condition:
        column = condition.split('<')[0]
        n = float(condition.split('<')[1])
        tickers = rcl.CL.read_data('registered_stocks.csv', '/Data/Chile/')['Ticker'].values.tolist()
        columns_in_data = df.columns.values.tolist()

        if column in columns_in_data:
            for ticker in tickers:
                datas, datelist = SC.list_by_date(ticker, column, df)
                datalist = datas[1:]
                filtered_data = []
                filtered_dates = []

                for i in range(len(datalist)):
                    if float(datalist[i]) < n:
                        filtered_data.append(datalist[i])
                        filtered_dates.append(datelist[i])

                if filtered_dates:
                    print(ticker, filtered_data, filtered_dates)
        else:
            print('The requested column is not in the data')
    elif '>' in condition:
        print('> condition not yet implemented')
    elif '=' in condition:
        print('= condition not yet implemented')
    else:
        print('Invalid condition format')

# Function to convert quarterly data to yearly data (Placeholder)
def quarters_to_years(data, dates):
    # Placeholder implementation
    return 0

# Function to calculate price to parameter ratios
# This includes options for various adjustments and filters, and can write results to a file if specified
def price_to_parameter(df, para, tofile=0, filename='Prices', years=1, corr_min=0, per_share=False, check_year=True, debug=False, forward=False, last=False, inv=False):
    if check_year:
        curr_date = datetime.datetime.now()

    prices_f = rcl.CL.read_data('Prices.csv')
    tickers = prices_f['ticker'].values.tolist()
    prices = prices_f['price'].values.tolist()
    shares = pd.to_numeric(prices_f['shares'], errors='coerce').tolist()
    parameter = []

    for ticker in tickers:
        corr = 1
        if corr_min != 0:
            a, _ = SC.list_by_date(ticker, 'Non-Controlling Profit', df)
            b, _ = SC.list_by_date(ticker, 'Net Profit', df)
            try:
                corr = (b[-1] - a[-1]) / float(b[-1])
            except (TypeError, ZeroDivisionError):
                corr = 1.0

        if per_share:
            corr /= float(shares[-1])

        datas, datelist = SC.list_by_date(ticker, para, df)
        if len(datelist) > 3 and datas[-1] == datas[-1]:
            if check_year and ((curr_date.year - datelist[-1] // 100) > 1 or (curr_date.month - (datelist[-1] % 100)) > 6):
                parameter.append(-999999999999999)
                if debug:
                    print(f"{ticker} does not report anymore (since {datelist[-1]})")
            else:
                try:
                    if all(datas[-i] == datas[-i] for i in range(1, 5)) and (sum(datas[-4:]) * corr) != 0:
                        if forward:
                            parameter.append(datas[-1] * 4 * corr)
                        elif last:
                            parameter.append(datas[-1] * corr)
                        else:
                            parameter.append(sum(datas[-4:]) * corr)
                    else:
                        parameter.append(np.nan * corr)
                except TypeError:
                    parameter.append(np.nan)
                    if debug:
                        print(f"Data not found for {ticker}")
        else:
            parameter.append(np.nan * corr)

    if not inv:
        p_para = np.divide(np.multiply(np.array(prices, dtype=np.float32), np.array(shares, dtype=np.float32)), np.array(parameter, dtype=np.float32))
    else:
        p_para = np.divide(np.array(shares, dtype=np.float32), np.multiply(np.array(prices, dtype=np.float32), np.array(parameter, dtype=np.float32)))

    if forward:
        prices_f[f'FP/{para}'] = p_para
    else:
        prices_f[f'p/{para}'] = p_para

    if tofile != 0:
        prices_f.to_csv(root_dir / Path(f'Data/Chile/{filename}.csv'), index=None, header=True)

    return prices_f

# Function to calculate the quick ratio (Current Assets / Current Liabilities)
# This function reads price data and adds the quick ratio to the data, optionally saving it to a file
def quick_ratio(df, tofile=0, filename='Prices', years=1):
    prices_f = rcl.CL.read_data('Prices.csv')
    tickers = prices_f['ticker'].values.tolist()
    quick_ratios = []

    for ticker in tickers:
        try:
            curr_lia = SC.list_by_date(ticker, 'Current Liabilities', df)[0][-1]
            curr_ass = SC.list_by_date(ticker, 'Current Assets', df)[0][-1]
            quick_ratios.append(float(curr_ass) / float(curr_lia))
        except ValueError:
            print(ticker, curr_lia, curr_ass)
            quick_ratios.append(np.nan)
            continue
    prices_f['QR'] = quick_ratios

    if tofile != 0:
        prices_f.to_csv(f'{filename}.csv', index=None, header=True)

    return prices_f

# Function to fetch current prices for a list of tickers and save them to a file
def prices_to_file(datafold):
    df = rcl.CL.read_data('Database_in_CLP.csv')
    tickers = rcl.CL.read_data('registered_stocks.csv')['Ticker'].values.tolist()
    prices = []
    shares = []
    first = True

    for ix, ticker in enumerate(tickers):
        ticker = ticker.replace(" ", "")
        if '(' in ticker:
            ticker = ticker.split('(')[0]
        quote, m = ld.live_quote_cl(ticker, driver=driver_fire)
        if first:
            time.sleep(5)
            first = False
        prices.append(quote)
        shares.append(SC.get_shares(df, ticker, quote, m))

    df = pd.DataFrame({'ticker': tickers, 'price': prices, 'shares': shares})
    df.to_csv('Data/Chile/Prices.csv', index=None, header=True)

# Function to calculate dividend yields based on dividend data
# It reads a file or DataFrame with dividend data, computes yields, and appends them to the prices file
import pandas as pd
from pathlib import Path


def dividend_yields(dfile='', datafolder='', dataf=[]):
    prices_f = rcl.CL.read_data('Prices.csv')

    if dfile:
        div_df = rcl.CL.read_data(f'Dividends/{dfile}')
    elif isinstance(dataf, pd.DataFrame):
        div_df = dataf
    else:
        print("Please enter a valid DataFrame or file")
        return

    # Standardize the column names and ensure the year is numeric
    div_df.columns = map(str.lower, div_df.columns)
    div_df['year'] = pd.to_numeric(div_df['year'])

    year_i = div_df['year'].min()
    year_f = div_df['year'].max()
    typexs = []

    # Identify and convert type columns to numeric
    for i in range(1, 4):
        typex = f'type {i}'
        if typex in div_df.columns:
            typexs.append(typex)
            div_df[typex] = pd.to_numeric(div_df[typex])

    # Calculate the total dividends and drop the type columns
    div_df['# div'] = div_df[typexs].sum(axis=1)
    div_df.drop(columns=typexs, inplace=True)

    # Ensure no duplicate tickers before the merge
    div_df.drop_duplicates(subset=['ticker'], inplace=True)
    prices_f.drop_duplicates(subset=['ticker'], inplace=True)

    for year in range(year_i, year_f + 1):
        # Filter dividend data for the current year
        temp = div_df[div_df['year'] == year].copy()

        # Rename columns for the current year
        temp.rename(columns={'dividends': f'dividends {year}', '# div': f'#div {year}'}, inplace=True)
        temp.drop(columns=['year'], inplace=True)

        # Merge with prices_f
        prices_f = pd.merge(prices_f, temp, how="left", on=["ticker"])

        # Ensure numeric conversion
        if f'dividends {year}' in prices_f.columns:
            prices_f[f'dividends {year}'] = pd.to_numeric(prices_f[f'dividends {year}'], errors='coerce')
        prices_f['price'] = pd.to_numeric(prices_f['price'], errors='coerce')

        # Calculate the dividend yield only if dividends data exists
        if f'dividends {year}' in prices_f.columns:
            prices_f[f'dividends {year}'] = 100. * prices_f[f'dividends {year}'] / prices_f['price']
        else:
            prices_f[f'dividends {year}'] = None  # Or handle it differently based on your needs

    # Ensure no duplicate tickers in the final DataFrame
    prices_f.drop_duplicates(subset=['ticker'], inplace=True)

    # Save the final DataFrame to a CSV file
    output_path = root_dir / Path(datafolder) / 'Prices.csv'
    prices_f.to_csv(output_path, index=False, header=True)

    print(f"Data saved to {output_path}")


# Function to create and clean a database for each ticker
# For each ticker, it generates a separate CSV file containing the relevant data columns
def create_cleaned_database(df, folder="", to_tickers=None):
    tickers = rcl.CL.read_data('registered_stocks.csv', to_tickers, 1)['Ticker'].values.tolist()

    for ticker in tickers:
        df_to_print = pd.DataFrame(columns=['Date'])

        for col in df.columns:
            try:
                datas, datelist = SC.list_by_date(ticker, col, df)
            except SystemExit:  # Raised if parameter does not exist
                continue

            df_temp = pd.DataFrame({'Date': np.array(datelist).astype(int), col: datas[1:]})
            df_to_print = pd.merge(df_to_print, df_temp, on='Date', how='outer')

        if folder and not os.path.isdir(folder):
            os.mkdir(folder)

        df_to_print.to_csv(f'{folder}/{ticker}_datab.csv')




#####################################################################


#print(df.loc[:, ['revenue','Date','TICKER']])
#start=time.time()
#df = SC.all_CLP(df)

#datas,datelist=SC.list_by_date(Ticker,'net profit',df) 
#quaters_to_years(datas,datelist)

#print(rcl.CL.read_data('registered_stocks.csv','/Data/Chile/'))


#### Below here what is needed to update data and scrape new fillings
if __name__ == "__main__":
    wd=os.getcwd()
    datafold='Data/Chile/'
    #
    # rcl.upandgetem('09','2021')
    # rcl.upandgetem('12','2021')
    # rcl.upandgetem('03','2022')
    #rcl.upandgetem('06','2022')
    #rcl.upandgetem('09','2022')
    #rcl.upandgetem('12','2022')
    #rcl.upandgetem('03','2023')
    #rcl.upandgetem('06','2023')
    #rcl.upandgetem('09','2023')
    #rcl.upandgetem('12','2023')
    #rcl.upandgetem('03','2024')
    #rcl.upandgetem('06','2024')

    # rcl.all_companies(rcl.lista,wd+datafold,'09','2023',update=1,updatemonth='03',updateyear='2013')
    # file_name='Database_Chile_Since_03-2013.csv'
    # print("Starting database conversion to clp")
    # df=rcl.CL.read_data(file_name)
    # start=time.time()
    # df = SC.all_CLP(df)
    # print(time.time()-start)
    # prices_to_file(wd+datafold)
    file_name='Database_in_CLP.csv'

    #print(rcl.scrap_dividends(wd+datafold,2018, types = [1,2,3], to_file = True))


    # # # # #Ticker='ZOFRI'
    # # # # #df=rcl.CL.read_data(file_name,wd+datafold,1)
    # # # # #a,dl=SC.list_by_date(Ticker,'Non-Controlling Profit',df)
    # # # # #print(a)
    # # # # #b,dl=SC.list_by_date(Ticker,'Net Profit',df)
    # # # # #print(b)
    # # # # #print((b[-1]-a[-1])/float(b[-1]))


    df=rcl.CL.read_data(file_name)
    price_to_parameter(df,'net profit',tofile=1, corr_min = 1,debug = False)
    price_to_parameter(df,'net profit',tofile=1, corr_min = 1,debug = False, forward = True)
    price_to_parameter(df,'net operating cashflows',tofile=1,corr_min = 1,debug = False)
    price_to_parameter(df,'net operating cashflows',tofile=1,corr_min = 1,debug = False,forward = True)
    price_to_parameter(df,'Equity',tofile=1,debug = False ,last = True) #Not enough data well reported by companies, use dividend_yields instead
    quick_ratio(df,tofile=1)
    dividend_yields(dfile = 'Dividends__2018_2024.csv', datafolder = datafold, dataf = [])



    ### FOR  Machine Learning
    ###create_cleaned_database(df, folder = "databases", to_tickers=wd+datafold)



    #find_condition(df,'current assets>0')
    #find_condition(df,'current assets=0')
    #find_condition(df,'current assets<1000000')
    #find_condition(df,'revenue<0')
    #find_condition(df,'dividends paid<0')
    #find_condition(df,'sales<0')
    #find_condition(df,'current assets+0')


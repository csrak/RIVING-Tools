import Search_chile as sc
import live_data as ld
import Requests_CL as rcl
import os
import pandas as pd

#
























'''
#This function creates a DataFrame, sorted by time, that includes the selected quantities of the selected enterprises and save it a given directory
#tickers = LIST of business tickers    #columns = LIST of quantities wanted (See the columns already in the Database)
#path = STRING of the directory path you want to save the file (BEWARE the path writing)
#df = PANDAS DATAFRAME of the Database you want to extract the information
def date_columns_summary(tickers,columns,df,path='',country='CL'):
    final_df = pd.DataFrame()
    for ticker in tickers:
        company_df = pd.DataFrame()
        for column in columns:
            datas,datelist = sc.list_by_date(ticker,column,df)
            column_df = pd.DataFrame({column:datas[1:]}, index = datelist)
            company_df = pd.concat([company_df,column_df], axis=1)
        company_df.columns = pd.MultiIndex.from_product([[ticker],company_df.columns])
        final_df = pd.concat([final_df,company_df], axis=1)
    name = 'Summary('+country+'-'+str(len(columns))+'-'+str(len(tickers))+')_since_'+str(final_df.index.min())[-2:]+'_'+str(final_df.index.min())[:-2]+'.csv'
    if path == '':
        path = '/ML_output/'
    final_df.to_csv(os.getcwd()+path+name)
#PD. If it gives you an permission denied error, make sure you have permission to change/write in the given directory

# W.I.P but it works
def read_summary(file_name,datafold):
    df = pd.read_csv(os.getcwd()+datafold+file_name, header= [0,1], index_col= 0)
    return df

#<><><><><><><><><><><><><><><><><><><><><><><><><><><>#

datafold = '/Data/Chile/'
file_name = 'Database_in_CLP.csv'
tickers_name = 'registered_stocks.csv'

df = rcl.CL.read_data(file_name,datafold)
tickers = rcl.CL.read_data(tickers_name,datafold)['Ticker'].tolist()

columns = ['revenue','operating profit','liabilities',
           'net operating cashflows','depreciation','net investing cashflows']

print('Start :D'+'\n')

date_columns_summary(tickers[0:2],columns[0:2],df)

datafold = '/ML_output/'
file_name = 'Summary(CL-6-201)_since_03_2013.csv'

df = read_summary(file_name,datafold)
print(df)
#company_df = df['IANSA']
#print(company_df)
#company_df['dates'] = company_df.index.to_numpy()
#company_df['dates'] = company_df['dates'].apply(lambda x: x.strftime('%Y%m'))
#dates = company_df.index.to_numpy()
#company_df = company_df.reset_index()
#company_df = company_df.apply(lambda x: x.strftime('%Y-%m'))

#new_index = pd.to_datetime(company_df.index.values, format='%Y%m')
#print(new_index)
#company_df = company_df.set_index(new_index)
#company_df = company_df.index.apply(lambda x: print(x))
#print(company_df)


print('End :D'+'\n')
'''

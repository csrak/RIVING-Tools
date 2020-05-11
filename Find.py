import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import Search_chile as SC

#Function to find a ticker per requisite
#That requisite should be written like column name + </=/> + float
#Future work will include ratios as column name 
 
def find_condition(df,condition):
	if condition.find('<')!=-1:
		column=condition[:condition.find('<')]
		n=float(condition[condition.find('<')+1:])
		tickers=rcl.CL.read_data('registered_stocks.csv','/Data/Chile/')
		tickers=tickers['Ticker'].values.tolist()
		columns_in_data=df.columns.values.tolist()
		if column in columns_in_data:
			#print('OK')
			for Ticker in tickers:
				datas,datelist=SC.list_by_date(Ticker,column,df) #Datos en miles de pesos
				datalist=datas[1:]
				data=[column]
				date=[]
				for i in range(len(datalist)):
					if float(datalist[i])<n:
						data.append(datalist[i])
						date.append(datelist[i])
					else:
						continue
				#print(Ticker,datas,datelist,len(datas),len(datelist))
				#print('Cut data',data,date,len(data),len(date))
				if len(date)!=0:
					print(Ticker,data,date,len(data),len(date))	
					
		else:
			print('The asking element is not in the data')
		
		#print(column,n,tickers)
		#print(columns_in_data)
	elif condition.find('>')!=-1:
		print(condition.find('>'))
	elif condition.find('=')!=-1:
		print(condition.find('='))
	else:
		print('No proper comparation')	
	#column=condition[]

def quarters_to_years(data,dates):
		#print(dates[i]/100,dates[i]//100,dates[i]%100)
		
	return 0

def Price_to_Earnings(df,years=1):
	tickers=rcl.CL.read_data('registered_stocks.csv','/Data/Chile/')	
	tickers=tickers['Ticker'].values.tolist()

	for Ticker in tickers:
		datas,datelist=SC.list_by_date(Ticker,'net profit',df) #Datos en miles de pesos
		datalist=datas[1:]
		#price=ld.yahoo_quote_CL(Ticker)
	
	return 0

def prices_to_file(datafold):
	#we pass present prices to a single file for faster screening, this file should be updated at least daily
	tickers=rcl.CL.read_data('registered_stocks.csv',datafold,1)
	tickers=tickers['Ticker'].values.tolist()
	prices=[]
	for Ticker in tickers:
		quote,m=ld.live_quote_cl(Ticker)
		prices.append(quote)
	df=pd.DataFrame()
	df['ticker']=tickers
	df['price']=prices
	print(df)
	df.to_csv(datafold+'Prices.csv', index = None, header=True)


#####################################################################

#datafold='/Data/Chile/'
#file_name='Database_in_CLP.csv'
#Ticker='AUSTRALIS'

#df=rcl.CL.read_data(file_name,datafold)
#print(df.loc[:, ['revenue','Date','TICKER']])
#start=time.time()
#df = SC.all_CLP(df)

#datas,datelist=SC.list_by_date(Ticker,'net profit',df) 
#quaters_to_years(datas,datelist)

#print(rcl.CL.read_data('registered_stocks.csv','/Data/Chile/'))
#Price_to_Earnings(df)
wd=os.getcwd()
datafold='/Data/Chile/'
prices_to_file(wd+datafold)



#find_condition(df,'current assets>0')
#find_condition(df,'current assets=0')
#find_condition(df,'current assets<1000000')
#find_condition(df,'revenue<0')
#find_condition(df,'dividends paid<0')
#find_condition(df,'sales<0')
#find_condition(df,'current assets+0')


import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import Search_chile as SC

#Function to find a ticker per requisit
#That requisit should be write like column name + </=/> + float
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

def quaters_to_years(data,dates):
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

def Prices():
	tickers=rcl.CL.read_data('registered_stocks.csv','/Data/Chile/')
	tickers=tickers['Ticker'].values.tolist()
	archivo=open('Prices.csv','w')
	archivo.write('price'+','+'Ticker'+'\n')
	for Ticker in tickers:
		price=ld.yahoo_quote_CL(Ticker)
		archivo.write(str(price)+','+Ticker+'\n')
	archivo.close()

#####################################################################

datafold='/Data/Chile/'
file_name='Database_in_CLP.csv'
Ticker='AUSTRALIS'

df=rcl.CL.read_data(file_name,datafold)
#print(df.loc[:, ['revenue','Date','TICKER']])
#start=time.time()
#df = SC.all_CLP(df)

#datas,datelist=SC.list_by_date(Ticker,'net profit',df) 
#quaters_to_years(datas,datelist)

#print(rcl.CL.read_data('registered_stocks.csv','/Data/Chile/'))
Price_to_Earnings(df)
Prices()



#find_condition(df,'current assets>0')
#find_condition(df,'current assets=0')
#find_condition(df,'current assets<1000000')
#find_condition(df,'revenue<0')
#find_condition(df,'dividends paid<0')
#find_condition(df,'sales<0')
#find_condition(df,'current assets+0')


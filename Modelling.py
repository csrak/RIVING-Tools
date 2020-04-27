import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import Search_chile as scl
#############
rate=10 #in % risk free rate
USDtoCLP=830.0 #Must be replaced with a function later

def basic_dcf(ticker,df):
	data='net operating cashflows'
	op_cash,datelist=scl.list_by_date(ticker, data,df)
	data='cash'
	cash,datelist=scl.list_by_date(ticker, data,df)
	data='liabilities'
	liabilities,datelist=scl.list_by_date(ticker, data,df)
	data='shares'
	shares,datelist=scl.list_by_date(ticker, data,df)
	quote,mk=ld.barron_quote_CL(ticker)
	print(shares)

	if len(shares)>1 and shares[-1]==shares[-1] and shares[-1]!=0 and quote!=0:
		if quote=='Try another scraper':
			shares=1
			print('Share not traded, value = 0 (changed to dummy 1)')
		elif abs((float(shares[-1])-float(mk/quote))/float(shares[-1]))<0.15:
			shares=shares[-1]
		else:
			shares=mk/quote
	else:
		if quote=='Try another scraper':
			shares=1
			print('Share not traded, value = 0 (changed to dummy 1)')
		else:
			try:
				shares=mk/quote
			except ZeroDivisionError:
				shares=1
				print('Share not traded, value = 0 (changed to dummy 1)')
	#print('quote= '+ str(quote))
	growth=[]
	#scl.plot_data_time(datelist,op_cash)
	op_cash.pop(0)
	for i in range(0,len(datelist)//4):
		opcash=op_cash[4*i]+op_cash[4*i+1]+op_cash[4*i+2]+op_cash[4*i+3]
		growth.append(opcash)
	print(growth)
	#growth=np.mean(growth)
	count=list(range(len(growth)))
	if len(growth)<2:
		return [ticker,'Not Found',str(quote)]
	fit = np.polyfit(count, growth, 1)
	#print(growth)
	value=opcash*(1+fit[1]/growth[-1])**10+cash[-1]-liabilities[-1]
	value=value/((1+(rate/100))**10)
	value=value/shares
	#print("right value = "+ str(value*20))
	#print("present value = "+str(quote))
	return [ticker,str(value*20),str(quote),str(mk)]

def model_all_0(df):
	tickers=rcl.CL.read_data('registered_stocks.csv','/Data/Chile/')
	tickers=tickers['Ticker'].values.tolist()
	all=[]
	for tick in tickers:
		all.append(basic_dcf(tick,df))
	df=pd.DataFrame(all,columns=['ticker','Estimate Value','Present Value','Market Cap.'])
	df.to_csv("./dcf_0_order.csv", sep=',',index=False)
	#print(*all, sep = "\n") 
		




datafold='/Data/Chile/'
file_name='Database_in_CLP.csv'
df=rcl.CL.read_data(file_name,datafold)
#basic_dcf('FALABELLA',df)
model_all_0(df)


############################ Ignore everything under ths line

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

	quote,market_cap=ld.live_quote_cl(ticker)
	if quote!=0:
		if len(shares)<3:
			shares=market_cap/quote
		elif market_cap==market_cap and market_cap!=0:
			if shares[-1]!=0 and abs(shares[-1]*quote-market_cap)/(shares[-1]*quote)<0.15:
				shares=shares[-1]
			elif shares[-2]!=0 and abs(shares[-2]*quote-market_cap)/(shares[-2]*quote)<0.15:
				shares=shares[-2]
			else:
				shares=market_cap/quote
		else:
			shares=shares[-1]
	else:
		shares=shares[-1]



	#print('quote= '+ str(quote))
	growth=[]
	#scl.plot_data_time(datelist,op_cash)
	op_cash.pop(0)
	for i in range(0,len(datelist)//4):
		opcash=op_cash[4*i]+op_cash[4*i+1]+op_cash[4*i+2]+op_cash[4*i+3]
		growth.append(opcash)
	print('growth')
	print(growth)
	#growth=np.mean(growth)
	count=list(range(len(growth)))
	if len(growth)<2:
		return [ticker,'Not Found',str(quote)]
	fit = np.polyfit(count, growth, 1)
	print('Fit')
	print(fit)
	value=growth[-1]+(fit[0])*10+cash[-1]-liabilities[-1]
	value=value/((1+(rate/100))**10)
	value=value/shares
	#print("right value = "+ str(value*20))
	#print("present value = "+str(quote))
	return [ticker,str(value*20),str(quote),str(shares)]

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
model_all_0(df)


############################ Ignore everything under ths line
ticker='AUSTRALIS'
basic_dcf(ticker,df)
data='net operating cashflows'
datas,datelist=scl.list_by_date(ticker, data,df)
scl.plot_data_time(datelist,datas)
data='revenue'
datas,datelist=scl.list_by_date(ticker, data,df)
scl.plot_data_time(datelist,datas)
data='net profit'
datas,datelist=scl.list_by_date(ticker, data,df)
scl.plot_data_time(datelist,datas)
data='assets'
data2='liabilities'
datas,datelist=scl.list_by_date(ticker, data,df)
datas2,datelist=scl.list_by_date(ticker, data2,df)
scl.plot_data_time(datelist,datas,datas2)

data='trade receivables'
data2='current assets'
datas,datelist=scl.list_by_date(ticker, data,df)
datas2,datelist=scl.list_by_date(ticker, data2,df)
scl.plot_data_time(datelist,datas,datas2)

data='current assets'
data2='current liabilities'
datas,datelist=scl.list_by_date(ticker, data,df)
datas2,datelist=scl.list_by_date(ticker, data2,df)
scl.plot_data_time(datelist,datas,datas2)
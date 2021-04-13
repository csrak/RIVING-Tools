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


def basic_dcf(ticker,df):
	ticker = ticker.upper()
	data='net operating cashflows'
	op_cash,datelist=scl.list_by_date(ticker, data,df)
	data='cash'
	cash,datelist=scl.list_by_date(ticker, data,df)
	data='liabilities'
	liabilities,datelist=scl.list_by_date(ticker, data,df)

	quote,market_cap=ld.live_quote_cl(ticker)
	shares=scl.get_shares(df,ticker,quote,market_cap)
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
		return [ticker,np.nan,str(quote)]
	fit = np.polyfit(count, growth, 1)
	print('Fit')
	print(fit)
	value=0
	for i in range(1,10):
		value+=((fit[0])*i)/((1+(rate/100))**i)
	value=(growth[-1]+value-liabilities[-1]+cash[-1])/shares
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
		


test = 1
file_name='Database_in_CLP.csv'
datafold='/Data/Chile/'
df=rcl.CL.read_data(file_name,datafold)
if test == 0:
	model_all_0(df)
############################ Ignore everything under ths line
else:
	ticker='ILC'
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
	data='cost of sales'
	data2='revenue'
	datas,datelist=scl.list_by_date(ticker, data,df)
	datas2,datelist=scl.list_by_date(ticker, data2,df)
	scl.plot_data_time(datelist,datas,datas2)
	data='equity'
	datas,datelist=scl.list_by_date(ticker, data,df)
	scl.plot_data_time(datelist,datas)
	data='inventories'
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

	data='cash'
	data2='current assets'
	datas,datelist=scl.list_by_date(ticker, data,df)
	datas2,datelist=scl.list_by_date(ticker, data2,df)
	scl.plot_data_time(datelist,datas,datas2)

	data='goodwill'
	data2='assets'
	datas,datelist=scl.list_by_date(ticker, data,df)
	datas2,datelist=scl.list_by_date(ticker, data2,df)
	scl.plot_data_time(datelist,datas,datas2)


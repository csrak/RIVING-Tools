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
	quote=ld.yahoo_quote_CL(ticker)
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
	fit = np.polyfit(count, growth, 1)
	#print(growth)
	value=opcash*(1+fit[1]/growth[-1])**10+cash[-1]-liabilities[-1]
	value=value/((1+(rate/100))**10)
	value=value/shares[-1]
	print("right value = "+ str(value*20))
	print("present value = "+str(quote))






datafold='/Data/Chile/'
file_name='Database_in_CLP.csv'
df=rcl.CL.read_data(file_name,datafold)
basic_dcf('FALABELLA',df)
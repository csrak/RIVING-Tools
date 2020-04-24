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

USDtoCLP=830.0 #Must be replaced with a function later

def basic_dcf(ticker,df):
	data='net operating cashflows'
	op_cash,datelist=scl.list_by_date(ticker, data,df)
	data='cash'
	cash,datelist=scl.list_by_date(ticker, data,df)
	data='liabilities'
	liabilities,datelist=scl.list_by_date(ticker, data,df)
	#quote=ld.yahoo_quote_CL(ticker)
	#print('quote= '+ str(quote))
	growth=[]
	scl.plot_data_time(datelist,op_cash)
	op_cash.pop(0)
	for i in range(0,len(datelist)//4):
		opcash=op_cash[4*i]+op_cash[4*i+1]+op_cash[4*i+2]+op_cash[4*i+3]
		growth.append(opcash)
	print(growth)
	#growth=np.mean(growth)
	growth = np.polyfit(range(len(growth)), growth, 1)
	value=opcash*(1+growth)**10+cash[-1]-liabilities[-1]
	print(value)





datafold='/Data/Chile/'
file_name='Database_in_CLP.csv'
df=rcl.CL.read_data(file_name,datafold)
basic_dcf('CENCOSUD',df)
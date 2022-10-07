import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import Search_chile as scl
import tensorflow as tf
from US_data_vis import list_4f_volume as l4v
#############
rate=10 #in % risk free rate

#Below some functions for formatting data from the API to be used for different modelling

class alphavalue_formatted_data:
	def __init__(self):
		data_frames = []
		api = ld.api_handler()
		self.api = api.alphavalue
	def get_weekly_quote(self, ticker):		
		ticker = ticker.lower()
		df = self.api.get_quotes(ticker, query = 'TIME_SERIES_WEEKLY_ADJUSTED')
		df["timestamp"] = pd.to_datetime(df["timestamp"])
		df.index = df.timestamp
		df = df[["adjusted close"]]
		df.rename(columns={'adjusted close':ticker}, inplace=True)
		#print(df)
		return df 
data = alphavalue_formatted_data()
df = data.get_weekly_quote('TSLA')
df2 = data.get_weekly_quote('IBM')
df = pd.merge(df,df2, how = 'outer',left_index=True, right_index=True)
print(df)
#plt.plot(df.index, df['quote'])
#plt.show()
quotes = df
#ticker_list=US.nasdaq_list(wd)
filenames = ['nflx','tsla','ibm']
#filenames = sample(ticker_list,600)
datafold = "/Data/US/4F/"
df = l4v(filenames, datafold,buyorsell="D", netbuy='y',datesample= 'W')
df = pd.merge(df,quotes, how = 'outer',left_index=True, right_index=True)
print("FInal Data Frame")
print(df)
#Get datetime in seconds
timestamp_s = df.index.map(pd.Timestamp.timestamp)
#Get fast fourier transform to see useful frequencies
fft = tf.signal.rfft(df['ibm'])
f_per_dataset = np.arange(0, len(fft))

n_samples_h = len(df['tsla'])
hours_per_year = 24*365.2524
years_per_dataset = n_samples_h/(hours_per_year)

f_per_year = f_per_dataset/years_per_dataset
plt.step(f_per_year, np.abs(fft))
plt.xscale('log')
plt.ylim(0, max(plt.ylim()))
plt.xlim([0.1, max(plt.xlim())])
plt.xticks([1, 365.2524], labels=['1/Year', '1/day'])
_ = plt.xlabel('Frequency (log scale)')
plt.show()


#Below printing some stock values vs net sales
'''
df.head()

fig,axis = plt.subplots()
# make a plot
axis.plot(df.index, df[filenames[0]], color="red", marker="o")
# set x-axis label
axis.set_xlabel("Date",fontsize=14)
# set y-axis label
axis.set_ylabel("Netflix",color="red",fontsize=14)
axis.set_ylim([0, 1])
axis2=axis.twinx()
# make a plot with different y-axis using second axis object
axis2.plot(df.index, df[filenames[1]],color="blue",marker="o")
axis2.set_ylabel("Tesla",color="blue",fontsize=14)
axis2.set_ylim([0, df[filenames[1]].max()*1.1])

axis3=axis.twinx()
# make a plot with different y-axis using second axis object
axis3.plot(df.index, df.quote,color="black",marker="o")
axis3.set_ylabel("Tesla stock",color="black",fontsize=14)
#axis3.set_ylim([0, 1])

plt.show()

print(df)
'''


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
		



'''
test = 1
file_name='Database_in_CLP.csv'
datafold='/Data/Chile/'
df=rcl.CL.read_data(file_name,datafold)
if test == 0:
	model_all_0(df)
############################ Ignore everything under ths line
else:
	ticker='ANDINA'
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
'''

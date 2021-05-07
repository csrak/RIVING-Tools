import pandas as pd
import datetime
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import Search_chile as SC

pd.options.mode.chained_assignment = None  # default='warn'

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

def price_to_parameter(df,para,tofile=0,filename='Prices',years=1,corr_min=0, per_share = False,check_year = True, debug = False, forward = False):	
	if check_year == True:
		curr_date = datetime.datetime.now()
	#corr min substracts earningsaccording to % ownership in minority stakes of the company, activated by default
	prices_f=rcl.CL.read_data('Prices.csv')	
	tickers=prices_f['ticker'].values.tolist()
	prices=prices_f['price'].values.tolist()
	shares=(pd.to_numeric(prices_f['shares'],errors='coerce')).tolist()
	parameter=[]
	p_para=[]

	for Ticker in tickers:
		corr=1
		if corr_min != 0:
			a,dl=SC.list_by_date(Ticker,'Non-Controlling Profit',df)
			b,dl=SC.list_by_date(Ticker,'Net Profit',df)
			try:
				corr = (b[-1]-a[-1])/float(b[-1])	#We calculate latest % ownerships discount and substract same for all previous quarters
											#Note this is "wrong" for previous quarters, but gives a more realistic view of the averaged future returns
											#since previous amount of ownerships do not matter for modelling of future earnings
			except TypeError:
				corr = 1. # Correction was not possible, we do it without
			except ZeroDivisionError:
				print("Div by zero: for "+ Ticker+ " and parameter " + para + " Non-controlling correction")
				corr = 1.
			if (pd.isnull(corr)):
				corr = 1.
		if per_share == True:
			corr = corr/float(shares[-1])
		datas,datelist=SC.list_by_date(Ticker,para,df) #Data in thousand of CLP
		if len(datelist)>3 and datas[-1]==datas[-1]:
			if check_year == True and ((curr_date.year-datelist[-1]//100)>1 or (curr_date.month-(datelist[-1]-(datelist[-1]//100)*100))>6):
				parameter.append(-999999999999999)#To differentiate garbage data we replace it with impossible number
				if debug == True:
					print (Ticker +' does not report anymore (since '+str(datelist[-1])+')\n')
			else:
				try: 
					if datas[-2]==datas[-2] and datas[-3]==datas[-3] and datas[-4]==datas[-4] and ((datas[-1]+datas[-2]+datas[-3]+datas[-4])*corr)!=0:
						if (forward ==True):
							parameter.append(datas[-1]*4*corr)
						else:
							parameter.append((datas[-1]+datas[-2]+datas[-3]+datas[-4])*corr)
					else:
						parameter.append(np.nan*corr)
				except TypeError:
					parameter.append(np.nan)
					print('Data no found for '+Ticker)
					if debug == True:
						print (para+':\n'+datas+':\n'+str(datelist)+'\n')
		else:
			parameter.append(np.nan*corr)
	#print(shares)
	p_para=np.divide(np.multiply(np.array(prices,dtype=np.float32),np.array(shares,dtype=np.float32)),np.array(parameter,dtype=np.float32))
	#prices_f[para]=parameter
	if (forward ==True):
		prices_f['FP/'+para]=p_para
	else:
		prices_f['p/'+para]=p_para
	if tofile!=0:
		prices_f.to_csv(filename+'.csv', index = None, header=True)
	return prices_f




def quick_ratio(df,tofile=0,filename='Prices',years=1):
	prices_f=rcl.CL.read_data('Prices.csv')
	tickers=prices_f['ticker'].values.tolist()
	quick_ratio=[]
	for Ticker in tickers:
		datas,datelist=SC.list_by_date(Ticker,'Current Liabilities',df) #Datos en miles de pesos
		try:
			curr_lia=float(datas[-1])
		except ValueError:
			curr_lia=np.nan
		datas,datelist=SC.list_by_date(Ticker,'Current Assets',df) #Datos en miles de pesos
		try:
			curr_ass=float(datas[-1])
		except ValueError:
			curr_ass=np.nan
		quick_ratio.append(curr_ass/curr_lia)	
	prices_f['QR']=quick_ratio
	if tofile!=0:
		prices_f.to_csv(filename+'.csv', index = None, header=True)
	return prices_f



def prices_to_file(datafold, series = 'A'): #We select the preferred series of the shares we want to consult, unique series are always included
	df=rcl.CL.read_data('Database_in_CLP.csv',datafold,1)
	#we pass present prices to a single file for faster screening, this file should be updated daily
	tickers=rcl.CL.read_data('registered_stocks.csv',datafold,1)
	tickers=tickers['Ticker'].values.tolist()
	prices=[]
	shares=[]
	for Ticker in tickers:
		quote,m=ld.live_quote_cl(Ticker,series)
		prices.append(quote)
		shares.append(SC.get_shares(df,Ticker,quote,m))
	df=pd.DataFrame()
	df['ticker']=tickers
	df['price']=prices
	df['shares']=shares
	print(df)
	df.to_csv('Prices.csv', index = None, header=True)


def dividend_yields(dfile = '', datafolder = '', dataf = []):
	prices_f=rcl.CL.read_data('Prices.csv')
	#tickers=prices_f['ticker'].values.tolist()
	#The dataframe or file should be in the format as generated by the function "scrap_dividends" in the Requests_CL library
	if dfile != '':
		div_df = rcl.CL.read_data(dfile, wd = 1,datafold = datafolder+'/Dividends/')
	elif not isinstance(dataf, pd.DataFrame):
		print("please enter a valid dataframe variable ( 'dataf =' ), or file (dfile = , folder =)")
		return
	else:
		div_df = dataf
	div_df.columns = map(str.lower, div_df.columns)
	div_df['year'] = pd.to_numeric(div_df['year'])
	year_i = div_df['year'].min()
	year_f = div_df['year'].max()
	typexs = []	
	prices_f['price'] = pd.to_numeric(prices_f['price'], errors = 'coerce')
	div_df['dividends'] = pd.to_numeric(div_df['dividends'], errors = 'coerce')
	for i in range(1,4):
		typex = 'type '+str(i)
		if typex in div_df:
			typexs.append(typex)		
			div_df[typex] = pd.to_numeric(div_df[typex])
	div_df['# div'] = div_df[typexs].sum(axis = 1)
	for typex in typexs:
		del div_df[typex]
	for year in range(year_i,year_f+1):
		temp=div_df[div_df['year']==year]
		temp.rename(columns = {'dividends': ('dividends ' + str(year)),'# div': ('#div ' + str(year))}, inplace = True)
		del temp['year']
		prices_f=pd.merge(prices_f, temp, how="left", on=["ticker"])
		#print(prices_f['price'],'*',prices_f[('dividends ' + str(year))])
		prices_f[('dividends ' + str(year))] = 100.*prices_f[('dividends ' + str(year))]/ prices_f['price'] 
	prices_f.to_csv('Prices.csv', index = None, header=True)
	

#####################################################################


#print(df.loc[:, ['revenue','Date','TICKER']])
#start=time.time()
#df = SC.all_CLP(df)

#datas,datelist=SC.list_by_date(Ticker,'net profit',df) 
#quaters_to_years(datas,datelist)

#print(rcl.CL.read_data('registered_stocks.csv','/Data/Chile/'))


wd=os.getcwd()
datafold='/Data/Chile/'

prices_to_file(wd+datafold,series = 'A')
file_name='Database_in_CLP.csv'
#Ticker='ZOFRI'
#df=rcl.CL.read_data(file_name,wd+datafold,1)
#a,dl=SC.list_by_date(Ticker,'Non-Controlling Profit',df)
#print(a)
#b,dl=SC.list_by_date(Ticker,'Net Profit',df)
#print(b)
#print((b[-1]-a[-1])/float(b[-1]))


df=rcl.CL.read_data(file_name,wd+datafold,1)
price_to_parameter(df,'net profit',tofile=1, corr_min = 1,debug = False)
price_to_parameter(df,'net profit',tofile=1, corr_min = 1,debug = False, forward = True)
price_to_parameter(df,'net operating cashflows',tofile=1,corr_min = 1,debug = False)
price_to_parameter(df,'net operating cashflows',tofile=1,corr_min = 1,debug = False,forward = True)
price_to_parameter(df,'dividends paid',tofile=1,debug = False) #Not enough data well reported by companies, use dividend_yields instead
quick_ratio(df,tofile=1)
dividend_yields(dfile = 'Dividends__2018_2021.csv', datafolder = wd+datafold, dataf = [])



#find_condition(df,'current assets>0')
#find_condition(df,'current assets=0')
#find_condition(df,'current assets<1000000')
#find_condition(df,'revenue<0')
#find_condition(df,'dividends paid<0')
#find_condition(df,'sales<0')
#find_condition(df,'current assets+0')


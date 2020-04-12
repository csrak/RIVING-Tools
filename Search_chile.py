import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl

#############

USDtoCLP=800

#Convert all USD to CLP and clean the table. Need current USD convertion
def all_CLP(df):
	n=len(df)
	#print(df.loc[[1500],:])
	#print(df.loc[[1501],:])
	#print(df.loc[[1502],:])
	convertion_rate=USDtoCLP
	for i in range(len(df)//int(len(df.columns))):
		for j in range(len(df.columns)-2):
			if float(df.iloc[3*i+2,j])==1.0:
				df.iloc[3*i,j]=str(float(df.iloc[3*i,j])*convertion_rate)
			elif float(df.iloc[3*i+2,j])==0.0:
				continue
			else:
				print('Not recognizible')
				
	for i in range(n//3):
		df.drop([n-3*i-1],inplace=True)
		
	return df

#Coment ####################################
#<><><><><><><><><><><><><><><><><><>#

def search_date(month,year,df,ticker):
    date=int(month)+int(year)*100
    a=df[df['Date'] == str(date)].index.tolist()
    b=df[df['TICKER'] == ticker].index.tolist()
    a=list(set(a) & set(b))
    a=min(a)
    return a,a+1

def date_range_gen(min,max):
	iyear=min//100
	imonth=min-iyear*100
	fyear=max//100
	fmonth=max-fyear*100
	datelist=[]
	year=iyear
	month=imonth
	#print(year)
	#print(month)
	while True:				
		datelist.append(year*100+month)
		if month==fmonth and year==fyear:
			break
		month=month+3
		if not month<=12:
			year=year+1
			month=3
	return datelist

#Returns list of data searched ordered by date
#Returns lists of dates
#If data not found for date data is NaN

def list_by_date(ticker, data,df): #Asks for ticker, name of data column and the dataframe after cutting 3rd row

    #col= df.loc[:, [data,'Date','TICKER']]	
	datelist=[]
	datas=[]
	#print(df.loc[[1500],:])
	df=df.loc[df['TICKER']==ticker]
	datelist=df['Date'].tolist()
	datelist=[int(i) for i in datelist]
	#print(df)
	dmax=max(datelist)
	dmin=min(datelist)
	datelist=date_range_gen(dmin,dmax)
	
	for i in range(len(datelist)):
		check=0
		for j in range(len(df)):
			if df.iloc[j]['Date']==str(datelist[i]):
				temp=df.iloc[j][data]
				datas.append(temp)
				check=1
		if check==0:
				datas.append(np.nan)
	#print(datas)
	#print(datelist)
	return datas,datelist
		
def plot_data_time():
	#nothing right now
	return 0



#############
#Testing how pandas and the table works

#start='2018/03'
datafold='/Data/Chile/'
file_name='Database_Chile_Since_03-2018.csv'

df=rcl.CL.read_data(file_name,datafold)#print(df.loc[:, ['revenue','Date','TICKER']])
df = all_CLP(df)
list_by_date('WATTS','revenue',df)


#search_date('06','2019',df,'WATTS')

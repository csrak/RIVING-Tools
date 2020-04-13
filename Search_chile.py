import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
#############

USDtoCLP=830 #Must be replaced with a function later

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
	
	data=rcl.check_parameter(data)
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
	n=len(datas)
	temp=[]
	for i in range(n//2):
		if data.lower() not in rcl.lista_instant:		
			if i==0:
				temp.append(float(datas[2*i]))
			elif float(datas[2*i+1])==1.0 and datelist[i]//100==datelist[i-1]//100: #Check if accumulated or only this trimester, then check if same year
				if float(datas[2*i-1])==0.0: #This means that we cannot substract directly, since previous point of data is 
					#print('Yes')
											# for one trimester, while now is cumulative for full year
					j=i #we set aux variable j to move through array
					value=float(datas[2*i]) #Initial value (Full year accumulated)				
					while True: #We loop through in reverse through the array, getting out when needed
						#print(value)
						j=j-1 #Counter		
						#print(datas[2*j])								
						value-=float(datas[2*j]) #We substract to ce accumulated value, the previous data point
						if float(datas[2*j+1])==1.0 or datelist[j-1]//100!=datelist[j]//100: 
							#If we have that we are now in a point which includes previous trimesters we can get out
							# Also if the next data point we would substract corresponds to another year
							# meaning we already substracted full year data 
							if j-1<0 and datelist[j]-100*(datelist[j]//100)!=3:
								return "Cumulative and Missing data at",datelist[j]
							break
						elif float(datas[2*j+1])==2.0:
							#If we have cumulative data but a point is missing we get out informing this needs to be manually adressed
							return "Cumulative and Missing data at",datelist[j]
					#We append the value of the total substraction
					temp.append(value)					
				elif float(datas[2*i-1])==2.0:				
					#If we have cumulative data but a point is missing we get out informing this needs to be manually adressed
					return "Cumulative and Missing data at",datelist[j]
				else:
					temp.append(float(datas[2*i])-float(datas[2*i-2]))
			else:
				temp.append(float(datas[2*i]))
		else:
			temp.append(float(datas[2*i]))

	datas=[i/1000 for i in temp]
	datas.insert(0,data)
	#print(datas)
	#print(datelist)
	return datas,datelist
		
def plot_data_time(datelist,data1, data2=[]):
	title1=data1[0]
	title2=''
	data=data1[1:]	
	if len(data2)==len(data1):
		title2=' over ' +data2[0]
		data2=data2[1:]
		data=[data[i]/data2[i] for i in range(len(data))]

	datelist=[(i[4:6]+'/'+i[0:4]) for i in [str(j) for j in datelist]]
	plt.title(title1 + title2)
	plt.xticks(np.arange(len(datelist)), datelist)
	plt.plot(data)
	plt.show()

	return 0



#############
#Testing how pandas and the table works

start='2018/03'
datafold='/Data/Chile/'
file_name='Database_Chile_Since_03-2016.csv'

df=rcl.CL.read_data(file_name,datafold)#print(df.loc[:, ['revenue','Date','TICKER']])
df = all_CLP(df)
tickers='DUNCANFOX'
datas,datelist=list_by_date(tickers,'current liabilities',df)
datas2,datelist=list_by_date(tickers,'current assets',df)
plot_data_time(datelist,datas,datas2)


#search_date('06','2019',df,'WATTS')

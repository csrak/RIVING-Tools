import pandas as pd
import os
import numpy as np
from pathlib import Path
import live_data as ld
import Requests_CL as rcl
from matplotlib import pyplot as plt
import time
import datetime
#############




#Convert all USD to CLP and clean the table. Need current USD convertion
#Now optimized and return an archive with the convertions
def all_CLP(df,const_USD = True):
	if const_USD == True:
		for trial in range(0,15):
			curr_date = datetime.datetime.now()- datetime.timedelta(days=trial)
			USDtoCLP = ld.USDtoCLPfunc(str(curr_date.month),str(curr_date.year),str(curr_date.day))
			if USDtoCLP > 0.0:
				break
	else:		
		curr_date = datetime.datetime.now()
		USDtoCLP = ld.USDtoCLPfunc(str(curr_date.month),str(curr_date.year),str(curr_date.day))#TBD: Change to variable USD conversion
	
	n=len(df)
	n2=len(df.columns)
	#print(df.loc[[1500],:])
	#print(df.loc[[1501],:])
	#print(df.loc[[1502],:])}
	counter=0
	for i in range(n2):
		df.iloc[:,i]=pd.to_numeric(df.iloc[:,i], errors='coerce')
		counter+=1
		if counter==n2-2:
			break
	convertion_rate=USDtoCLP
	df2=df.iloc[:,0:-2].to_numpy()
	df0=df2[0::3,:] #Data
	df1=df2[1::3,:] #Acumulated or isntant indexes
	df2=df2[2::3,:]*convertion_rate #Converter of currency
	df_f=np.multiply(df0,(np.add(df2,1.0)))	
	#for i in range(len(df)//3):
	#	for j in range(len(df.columns)-2):
	#			if float(df.iloc[3*i+2,j])==1.0:
	#			df.iloc[3*i,j]=str(float(df.iloc[3*i,j])*convertion_rate)
	#		elif float(df.iloc[3*i+2,j])==0.0:
	#			continue
	#		else:
	#			print('Not recognizible')
	final_array=np.vstack((df_f[0], df1[0]))	
	final_df_temp=pd.DataFrame()
	final_df_temp['Date']=df['Date']
	final_df_temp['TICKER']=df['TICKER']
	tick=df['TICKER'].tolist()
	date=df['Date'].tolist()
	del tick[n-1]
	del date[n-1]
	for i in range(1,n//3):
		final_array=np.vstack((final_array, df_f[i]))
		final_array=np.vstack((final_array, df1[i]))
		del tick[n-1-3*i]
		del date[n-1-3*i]
	counter=0
	final_df=pd.DataFrame()
	final_array[:,0]
	for column in df:		
		final_df[column]=final_array[:,counter]
		#print(final_array[:,i])
		counter+=1
		if counter==(len(df.columns)-2):
			break
	final_df['Date']=date
	final_df['TICKER']=tick
	wd = os.getcwd()
	datafold=wd+'/Data/Chile/'
	final_df.to_csv(datafold+'Database_in_CLP.csv', sep=','  , index = None, header=True)
	return final_df


def search_date(month,year,df,ticker):
    date=int(month)+int(year)*100
    a=df[df['Date'] == str(date)].index.tolist()
    b=df[df['TICKER'] == ticker].index.tolist()
    a=list(set(a) & set(b))
    a=min(a)
    return a,a+1

#Generates a range of month dates, option a is for quarters (by 3 months) option b is for all months
def date_range_gen(min,max,op='a'):
	iyear=min//100
	imonth=min-iyear*100
	fyear=max//100
	fmonth=max-fyear*100
	datelist=[]
	year=iyear
	month=imonth
	#print(year)
	#print(month)
	if op=='a':
		while True:				
			datelist.append(year*100+month)
			if month==fmonth and year==fyear:
				break
			month=month+3
			if not month<=12:
				year=year+1
				month=3
	elif op=='b':
		while True:				
			datelist.append(year*100+month)
			if month==fmonth and year==fyear:
				break
			month=month+1
			if not month<=12:
				year=year+1
				month=1
	return datelist

#Returns list of data searched ordered by date
#Returns lists of dates
#If data not found for date data is NaN
def bank_list_by_date(ticker, data,df,month,year):
	#data=data.lower()
	#df_par=rcl.CL.read_data('bank_parameters_since_'+str(month)+'-'+str(year)+'.csv','/Data/Chile/Banks/')
	#params=df_par['0'].tolist()+df_par['1'].tolist()+df_par['2'].tolist()
	#if data not in [a.lower() for a in params]:
	#	print('Current available parameters:\n')
	#	print(*params, sep='\n')
	#	raise SystemExit('Please select a parameter from the available database') 
	df=df.loc[df['ticker']==ticker]
	df=df.loc[:,[data,'ticker','date']]
	datelist=df['date'].tolist()
	if len(datelist)==0:
		return [data],[]
	datelist=[int(i) for i in datelist]
	dmax=max(datelist)
	dmin=min(datelist)
	datelist=date_range_gen(dmin,dmax,'b')
	datas=[]
	datas.append(data)
	for i in range(len(datelist)):
		check=0
		for j in range(len(df)):
			if df.iloc[j]['date']==str(datelist[i]):
				temp=float(df.iloc[j][data])
				datas.append(temp)
				check=1
		if check==0:
				datas.append(np.nan)
	return datas,datelist

def list_by_date(ticker, data,df,month=0,year=0): 
	#month and year are used only for indicating bak database to be used, should not be set for rest of databases
	#Asks for ticker, name of data column and the dataframe after cutting 3rd row
	#Returns list with element 0 being the name of the data, and the ret being the data
	#Also returns list of dates, both in same order
	if month!=0 or year!=0:
		return bank_list_by_date(ticker, data,df,month,year)
	data=rcl.check_parameter(data)
    #col= df.loc[:, [data,'Date','TICKER']]	
	datelist=[]
	datas=[]
	#print(df.loc[[1500],:])
	df=df.loc[df['TICKER']==ticker]
	df=df.loc[:,[data,'TICKER','Date']]
	datelist=df['Date'].tolist()
	if len(datelist)==0:
		return [data],[]
	datelist=[int(i) for i in datelist]
	
	#print(df)
	dmax=max(datelist)
	dmin=min(datelist)
	datelist=date_range_gen(dmin,dmax)
	#print(datelist)
	for i in range(len(datelist)):
		check=0
		for j in range(len(df)):
			if df.iloc[j]['Date']==str(datelist[i]):
				temp=df.iloc[j][data]
				datas.append(float(temp))
				check=1
		if check==0:
				datas.append(np.nan)
				datas.append(np.nan)
	#print(datas)
	#print(datas)
	n=len(datas)
	#print(datas)
	temp=[]
	for i in range(n//2):
		if data.lower() not in rcl.lista_instant:	
			if i==0:
				temp.append(float(datas[2*i]))
			elif (datas[2*i+1]==  11.0) and (datelist[i]//100)==(datelist[i-1]//100): #Check if accumulated or only this trimester, then check if same year
				if datas[2*i-1]==10: #This means that we cannot substract directly, since previous point of data is 
					#print('Yes')
											# for one trimester, while now is cumulative for full year
					j=i #we set aux variable j to move through array
					value=float(datas[2*i]) #Initial value (Full year accumulated)				
					while True: #We loop through in reverse through the array, getting out when needed
						#print(value)
						j=j-1 #Counter		
						#print(datas[2*j])								
						value-=float(datas[2*j]) #We substract to ce accumulated value, the previous data point
						if float(datas[2*j+1])==11 or datelist[j-1]//100!=datelist[j]//100: 
							#If we have that we are now in a point which includes previous trimesters we can get out
							# Also if the next data point we would substract corresponds to another year
							# meaning we already substracted full year data data is missing and we stop the process
							if j-1<0 and datelist[j]-100*(datelist[j]//100)!=3:
								return "Cumulative and Missing data at",[datelist[j]]
							break
						elif datas[2*j+1]==12:
							#If we have cumulative data but a point is missing we get out informing this needs to be manually adressed
							return "Cumulative and Missing data at",[datelist[j]]
					#We append the value of the total substraction
					temp.append(value)					
				elif datas[2*i-1]==12:				
					#If we have cumulative data but a point is missing we get out informing this needs to be manually adressed
					print('Cumulative and Missing data at', datelist[i]," for ",ticker)
					temp.append(np.nan)
				else:
					temp.append(float(datas[2*i])-float(datas[2*i-2]))
			else:
				temp.append(float(datas[2*i]))
		else:
			temp.append(float(datas[2*i]))

	datas=[i for i in temp]
	datas.insert(0,data)
	#print(datas)
	#print(datelist)
	return datas,datelist

def get_shares(df,ticker,quote,market_cap):
	data='shares'
	shares_l,datelist=list_by_date(ticker, data,df)
	shares=np.nan
	for i in range(3):
		if quote!=0:
			if len(shares_l)<3:
				shares=market_cap/quote
				break
			elif market_cap==market_cap and market_cap!=0:
				if shares_l[-1]!=0 and abs(shares_l[-1]*quote-market_cap)/(shares_l[-1]*quote)<0.15:
					shares=shares_l[-1]
					break
				else:
					shares=market_cap/quote
					shares_l.pop(-1)
			else:
				shares=shares_l[-1]
		else:
			shares=shares_l[-1]
		
	return shares


def plot_data_time(datelist,data1, data2=[]):
	title1=data1[0]
	title2=''
	data=data1[1:]	
	if len(data2)==len(data1):
		title2=' over ' +data2[0]
		data2=data2[1:]
		for i in range(len(data)):
			try:
				data[i]=data[i]/data2[i]
			except ZeroDivisionError:
				data[i]=np.nan
				print(str(data2[i]) + ' at: '+str(datelist[i])[0:4]+ ' / ' + str(datelist[i])[4:6])
	datelist=[(i[4:6]+'/'+i[0:4]) for i in [str(j) for j in datelist]]
	plt.title(title1 + title2)
	plt.xticks(np.arange(len(datelist)), datelist)
	plt.plot(data)
	
	plt.show()
	return 0

def DSO(ticker,df,date=0):#Days Sales Outsanding
	revenue,datelist1=list_by_date(ticker, 'revenue',df)
	accounts_receivable,datelist2=list_by_date(ticker, 'Trade Receivables',df)
	DSO=[]
	if datelist1==datelist2:
		for i in range(len(datelist1)): #Credit sales assumed not cash revenue
			DSO.append(accounts_receivable[i+1]/revenue[i+1]*91.0)
	return DSO, datelist1
	

def DIO(ticker,df,date=0):#Days Inventories Outsanding
	COS,datelist1=list_by_date(ticker, 'Cost of sales',df)
	inventories,datelist2=list_by_date(ticker, 'inventories',df)
	DIO=[]
	DIO.append(0)
	if datelist1==datelist2:
		for i in range(len(datelist1)-1): #Credit sales assumed not cash revenue
			DIO.append((inventories[i+2]+inventories[i+1])/COS[i+2]*91.0)
	return DIO, datelist1

def DPO(ticker,df,date=0):#Days Sales Outsanding
	COS,datelist1=list_by_date(ticker, 'Cost of sales',df)
	current_payables,datelist2=list_by_date(ticker, 'current payables',df)
	DPO=[]
	if datelist1==datelist2:
		for i in range(len(datelist1)): #Credit sales assumed not cash revenue
			DPO.append(current_payables[i+1]/COS[i+1]*91.0)
	return DPO, datelist1

def rDSO(ticker,df,date=0):#Days Sales Outsanding with calculated Credit Sales (Revenue - Cash Sales)
	revenue,datelist1=list_by_date(ticker, 'revenue',df)
	cfs,datelist2=list_by_date(ticker, 'Cash from sales',df)
	cfy,datelist1=list_by_date(ticker, 'Cash from yield',df)	
	cfr,datelist2=list_by_date(ticker, 'Cash from rent',df)
	accounts_receivable,datelist2=list_by_date(ticker, 'Trade Receivables',df)
	rDSO=[]
	yrDSO=[]
	if datelist1==datelist2:
		for i in range(len(datelist1)):
			print('cash '+str(cfs[i+1]))
			print('revenue '+ str(revenue[i+1]))
			if np.isnan(cfy[i+1]):
				print('Cash from yield not found at '+str(datelist1[i])+', Assumed 0')
				cfy[i+1]=0
			if np.isnan(cfr[i+1]):
				print('Cash from rent not found at '+str(datelist1[i])+', Assumed 0')
				cfr[i+1]=0
			credit_sales=revenue[i+1]-cfs[i+1]-cfy[i+1]-cfr[i+1] #Credit sales assumed not cash revenue
			rDSO.append(accounts_receivable[i+1]/credit_sales*91.0)
			if i>0 and i%4==0:
				print(i)
				yrDSO.append((rDSO[i-1]+rDSO[i-2]+rDSO[i-3]+rDSO[i-4])/4)
	return rDSO, datelist1,yrDSO
	
def rec_turn(ticker,df,date=0):#Receivable Turnover
	revenue,datelist1=list_by_date(ticker, 'revenue',df)
	cfs,datelist2=list_by_date(ticker, 'Cash from sales',df)
	cfy,datelist1=list_by_date(ticker, 'Cash from yield',df)	
	cfr,datelist2=list_by_date(ticker, 'Cash from rent',df)
	accounts_receivable,datelist2=list_by_date(ticker, 'Trade Receivables',df)
	rec_turn=[]
	if datelist1==datelist2:
		for i in range(len(datelist1)):
			if np.isnan(cfy[i+1]):
				print('Cash from yield not found at '+str(datelist1[i])+', Assumed 0')
				cfy[i+1]=0
			if np.isnan(cfr[i+1]):
				print('Cash from rent not found at '+str(datelist1[i])+', Assumed 0')
				cfr[i+1]=0
			credit_sales=revenue[i+1]-cfs[i+1]-cfy[i+1]-cfr[i+1] #Credit sales assumed not cash revenue
			rec_turn.append(credit_sales/accounts_receivable[i+1])
	return rec_turn, datelist1

def CCO(df): #W.I.P
	start=time.time()
	dios,datelist=DIO('SQM',df)
	dsos,datelist=DSO('SQM',df)
	dpos,datelist=DPO('SQM',df)
	for i in range(len(datelist)):
		print(dios[i]+dsos[i]-dpos[i])
	print(time.time()-start)
	return 'not yet'



	
	




#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>#
#############

#datafold='/Data/Chile/Banks/'
#file_name='bank_database_since_11-2012.csv'

datafold='/Data/Chile/'
file_name='Database_Chile_Since_03-2013.csv'

df=rcl.CL.read_data(file_name,datafold)#print(df.loc[:, ['revenue','Date','TICKER']])
#start=time.time()
df = all_CLP(df)
#print(time.time()-start)

#tickers='BSANTANDER'
#datas,datelist=list_by_date(tickers,'1000000',df,12,2012)
#datas2,datelist=list_by_date(tickers,'2000000',df,12,2012)
#print(datas)
#print(datelist)
#plot_data_time(datelist,datas,datas2)
#datas,datelist=list_by_date(tickers,'1309100',df,12,2012)
#print(datas)
#print(datelist)
#plot_data_time(datelist,datas)
#datas2,datelist=list_by_date(tickers,'liabilities',df)
#print(datelist)
#plot_data_time(datelist,datas,datas2)
#start=time.time()
#print('rDSO:')
#print(rDSO('SQM',df))
#print(time.time()-start)
#start=time.time()
#print('rec_turn:')
#print(rec_turn('SQM',df))
#print(time.time()-start)
#search_date('06','2019',df,'WATTS')

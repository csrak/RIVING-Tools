import pandas as pd
import os
from pathlib import Path
import live_data as ld
import Requests_CL as rcl

#############

#Convert all USD to CLP and clean the table. Need current USD convertion
def all_CLP(df):
	n=len(df)
	convertion_rate=800
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



#############
#Testing how pandas and the table works

#start='2018/03'
datafold='/Data/Chile/'
file_name='Database_Chile_Since_03-2018.csv'

df=rcl.CL.read_data(file_name,datafold)
#print(df.loc[:, ['revenue','Date','TICKER']])


def search_date(month,year,df,ticker):
    date=int(month)+int(year)*100
    a=df[df['Date'] == str(date)].index.tolist()
    b=df[df['TICKER'] == ticker].index.tolist()
    a=list(set(a) & set(b))
    a=min(a)
    return a,a+1

search_date('06','2019',df,'WATTS')

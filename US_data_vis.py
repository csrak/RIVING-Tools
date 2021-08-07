import requests
import pandas as pd
import os
import glob
import numpy as np
from lxml import etree
import lxml.html as lh
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import US_data as US
import Chile_Data as CL
import datetime
import matplotlib.pyplot
import matplotlib.dates
from random import sample
import sys

def list_of_dates(init, end, numdays):    
    date_generated = [init + datetime.timedelta(days=x) for x in range(0, (end-init).days+numdays,numdays)]
    return date_generated

def plot_4f_volume(filename,datafold, netbuy = 'n',  buyorsell = "D",  datesample = 'W-Mon' ):
    #dtype = {"date": }|
    database = CL.read_data(filename,datafold)    
    database.set_index('date', inplace=True)
    database.index = pd.to_datetime(database.index, infer_datetime_format=True)
    datas = pd.DataFrame()
    if netbuy == 'y':
        database1 = database[database["acquiredordisposed"]=="D"]
        database2 = database[database["acquiredordisposed"]=="A"]
        
        database1.loc[:,"shares"]=-1*pd.to_numeric(database1["shares"])
        database1.loc[:,"price"]=pd.to_numeric(database1["price"]) 
        
        database2.loc[:,"shares"]=pd.to_numeric(database2["shares"])
        database2.loc[:,"price"]=pd.to_numeric(database2["price"]) 
        database2 = database2[database2['price'] > 0]
        datas1=database1.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
        datas2=database2.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
        
        datas  = (datas1.add(datas2, fill_value=0))
    else:
        database = database[database["acquiredordisposed"]==buyorsell]
        database.loc[:,"shares"]=pd.to_numeric(database["shares"])
        database.loc[:,"price"]=pd.to_numeric(database["price"]) 
        datas=database.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
    #datas = database.groupby([pd.Grouper(level = 'date', freq='W-MON'),"issuerTradingSymbol"])["shares"].sum().resample('W-Mon')
    #datas=database.groupby('issuertradingsymbol').resample('W-Mon')["shares"].sum() 
    datas.index = [a[1] for a in datas.index] 
    #print(datas)
    #print(min(datas.index.tolist()))
    #print(max(datas.index.tolist()))
    datelist= list_of_dates(min(datas.index.tolist()),max(datas.index.tolist()),7)
    datas = datas.reindex(datelist, fill_value=0)
    datelist = matplotlib.dates.date2num(datas.index.tolist())
    matplotlib.pyplot.plot_date(datelist, [float('nan') if x==0 else x for x in datas.tolist()])
    matplotlib.pyplot.show()
    #print(datas)
    #print(datas)

def plot_list_4f_volume(filenames,datafold, netbuy = 'n',  buyorsell = "D",  datesample = 'W-Mon', normalize = "yes"):
    #Filenames should be a list of tickers
    #dtype = {"date": }|
    with pd.option_context('mode.chained_assignment', None): #TO avoid ugly warnings related to loc, and views, doesn't matter
        for name in filenames:
            fold = datafold+name+"/"
            filename = "4f_"+name+".csv"
            try:
                database = CL.read_data(filename,fold)    
            except SystemExit:
                print("File ",filename, " not found in ", fold, ". Skipped.")
                continue
            except KeyboardInterrupt:
                sys.exit()
            if database.empty:
                print("DataFrame for ", name," is empty, skipping.")
                continue
            database.set_index('date', inplace=True)
            try:
                database.index = pd.to_datetime(database.index, infer_datetime_format=True)
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                continue
            datas = pd.DataFrame()
            if netbuy == 'y':
                database1 = database[database["acquiredordisposed"]=="D"]
                database2 = database[database["acquiredordisposed"]=="A"]

                database1.loc[:,"postransaction"]=pd.to_numeric(database1["postransaction"]) 
                database1.loc[:,"staticholdings"]=pd.to_numeric(database1["staticholdings"]) 

                database1.loc[:,"shares"]=-1*pd.to_numeric(database1["shares"])
                database1.loc[:,"price"]=pd.to_numeric(database1["price"]) 
                
                database2.loc[:,"shares"]=pd.to_numeric(database2["shares"])
                database2.loc[:,"price"]=pd.to_numeric(database2["price"]) 
                database2 = database2[database2['price'] > 0]

                holders = database1.groupby('rptownercik')['shares'].sum()#the sum doesn't matter it is just to know the CIK's
                
                list_holds = pd.DataFrame()
                resampled = database1.resample(datesample).last()
                print(resampled)
                c = 1
                for cik in  holders.index.to_list():
                    temp= resampled[ resampled['rptownercik'] == cik]
                    temp = temp[['postransaction','staticholdings']].copy()
                    if (c == 1):
                        list_holds =  temp

                        c = 0
                    else:
                         list_holds=list_holds.add(temp, fill_value=-1)
                #total_hold = sum(list_holds)
                #totalhold = holders['staticholdings'].sum()
                print(list_holds)
                print('----------------------------------------------')
                datas1=database1.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
                datas2=database2.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
                
                datas  = (datas1.add(datas2, fill_value=0))
            else:
                database = database[database["acquiredordisposed"]==buyorsell]
                database.loc[:,"shares"]=pd.to_numeric(database["shares"])
                database.loc[:,"price"]=pd.to_numeric(database["price"]) 
                datas=database.groupby('issuertradingsymbol').resample(datesample)["shares"].sum()
            #datas = database.groupby([pd.Grouper(level = 'date', freq='W-MON'),"issuerTradingSymbol"])["shares"].sum().resample('W-Mon')
            #datas=database.groupby('issuertradingsymbol').resample('W-Mon')["shares"].sum() 
            datas.index = [a[1] for a in datas.index] 
            #print(datas)
            #print(min(datas.index.tolist()))
            #print(max(datas.index.tolist()))
            #datelist= list_of_dates(min(datas.index.tolist()),max(datas.index.tolist()),7)
            #datas = datas.reindex(datelist, fill_value=0)
            datelist = matplotlib.dates.date2num(datas.index.tolist())
            if normalize == "yes":
                datas=datas/datas.abs().sum()
            matplotlib.pyplot.plot_date(datelist, [float('nan') if x==0 else x for x in datas.tolist()], label=name)
            matplotlib.pyplot.legend()
        matplotlib.pyplot.show()
    #print(datas)


company = "tsla"
filen = "4f_"+company+".csv"
datafold = "/Data/US/4F/"+company+"/"
#plot_4f_volume(filen, datafold,buyorsell="D", netbuy='y',datesample= 'M')
datafold = "/Data/US/4F/"
wd=os.getcwd()
ticker_list=US.nasdaq_list(wd)
filenames = ['nflx','tsla']
#filenames = sample(ticker_list,600)
plot_list_4f_volume(filenames, datafold,buyorsell="D", netbuy='y',datesample= 'M')


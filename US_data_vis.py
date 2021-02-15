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

def list_of_dates(init, end, numdays):    
    date_generated = [init + datetime.timedelta(days=x) for x in range(0, (end-init).days+numdays,numdays)]
    return date_generated

def plot_4f_volume(filename,datafold, buyorsell):
    #dtype = {"date": }|
    database = CL.read_data(filename,datafold)
    database = database[database["acquiredordisposed"]==buyorsell]
    database.set_index('date', inplace=True)
    database.index = pd.to_datetime(database.index)
    database["shares"]=pd.to_numeric(database["shares"])
    #datas = database.groupby([pd.Grouper(level = 'date', freq='W-MON'),"issuerTradingSymbol"])["shares"].sum().resample('W-Mon')
    datas=database.groupby('issuerTradingSymbol').resample('W-Mon')["shares"].sum()    
    datas.index = [a[1] for a in datas.index] 
    print(datas)
    #print(min(datas.index.tolist()))
    #print(max(datas.index.tolist()))
    datelist= list_of_dates(min(datas.index.tolist()),max(datas.index.tolist()),7)
    datas = datas.reindex(datelist, fill_value=0)
    datelist = matplotlib.dates.date2num(datas.index.tolist())
    matplotlib.pyplot.plot_date(datelist, datas.tolist())
    matplotlib.pyplot.show()
    print(datas)
    #print(datas)

company = "NFLX"
filen = "4f_"+company+".csv"
datafold = "/Data/US/4F/NFLX/"
plot_4f_volume(filen, datafold,"D")

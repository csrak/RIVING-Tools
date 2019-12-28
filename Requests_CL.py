import requests
import lxml.html as lh
import pandas as pd
import os
from pathlib import Path
from difflib import SequenceMatcher
import re
import Chile_Data as CL



##
##
# Format_Data -> Obtains and formats data already downloaded from updated databases and creates joint database
#  ###### USE ONLY IF UPDATE DATA THROWS NO ERRORS  ##############
####################################################################

def Format_Data(month, year):

    #Reads files downloaded from databases

    file_name='registered_stocks_'+month+'-'+year+'.csv'
    datafold='\\Data\\Chile\\'
    ruts=CL.read_data(file_name,datafold)

    file_name='registered_stocks_mw.csv'
    tick=CL.read_data(file_name,datafold)

    # Then compares both to select right tickers 
    ruts=CL.Tick2Rut(ruts,tick)

    # Finally saves results 
    wd=os.getcwd()
    file_name='registered_stocks_TICKER.csv'
    datafold=wd+'\\Data\\Chile\\'
    ruts.to_csv(datafold+file_name, index = None, header=True)
    #Returns results in case of use
    return ruts

##
##
# Update_Data -> Updates data already scraping from multiple databases (Needs Format_Data to be used)
##

def Update_Data(month, year):

    # First updates ticker list from MW or another selected url
    CL.scrap_mw()
    # Then updates list of Registered Businesses with ruts
    CL.scrap_rutlist(month,year)
    #Nothing else for now
    return 0


month='03'
year='2019'
Format_Data(month,year)

file_name='registered_stocks_TICKER.csv'
datafold='\\Data\\Chile\\'
df=CL.read_data(file_name,datafold)
df=df.iloc[0:5,:]
CL.get_ruts(df)
print(df)
a=CL.scrap_company_Links(df,month,year)
print(a, sep='\n')
flinks=[]
fnames=['0']*len(a)
for i in range (0,len(a)):
    if a[i]=='Not Found':
        filet=999
    elif df.loc[i,'File']== 'Archivo XBRL':
        filet=1 # file type XBLR
        fnames[i]=df.loc[i,'Ticker']+'_'+month+'-'+year+'.zip'
    else:
        filet=0 # File type PDF
        fnames[i]=df.loc[i,'Ticker']+'_'+month+'-'+year+'.pdf'
    flinks.append(CL.scrap_file_links(a[i],filet))

print(flinks)
print(len(flinks))
CL.scrap_fillings(flinks,fnames)



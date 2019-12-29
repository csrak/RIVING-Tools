import requests
import lxml.html as lh
import pandas as pd
import os
from pathlib import Path
from difflib import SequenceMatcher
import re
import tabula
import PyPDF2
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
    ruts=CL.Tick2Rut(ruts,tick) #Not  used anymore, since compares names manually

    # Finally saves results 
    wd=os.getcwd()
    file_name='registered_stocks_TICKER'+month+'-'+year+'.csv'
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

def get_fillings(month,year,df): 
    Format_Data(month,year)
    CL.get_ruts(df)
    print(df)
    a=CL.scrap_company_Links(df,month,year)
    #print(a, sep='\n')
    filet=-1
    flinks=[]
    fnames=['0']*len(a)
    for i in range (0,len(a)):
        if a[i]=='Not Found':
            filet=999
        temp=CL.scrap_file_links(a[i],filet)
        flinks.append(temp[0])
        filet=temp[1]
        if  filet==1:# file type XBLR
            fnames[i]=month+'-'+year+'\\'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.zip'
        elif filet==0:
            fnames[i]=month+'-'+year+'\\'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.pdf'
        

    print(wd+datafold+month+'-'+year)    
    if not os.path.exists(wd+datafold+month+'-'+year):
        os.mkdir(wd+datafold+month+'-'+year)

    #print(flinks)
    #print(len(flinks))
    CL.scrap_fillings(flinks,fnames)    


def read_pdf_fil(file_name,datafold):
    filepdf = open(datafold+file_name, 'rb')
    # pdf reader object
    filling = PyPDF2.PdfFileReader(filepdf)
    # # number of pages in pdf
    print(filling.numPages)
    # # a page object
    table=[]
    #print(page.extractText())
    for i in range (0,filling.numPages):
        page = filling.getPage(i)
        if 'PASIVOS Y PATRIMONIO NETO' in page.extractText():
            table = tabula.read_pdf(datafold+file_name,pages=i)
            print(table)
            ix=CL.getIndexes(table,'Pagos Anticipados')
            
            print(ix)
    #print(df.loc[3,'Pagos Anticipados'])
    

month='03'
year='2019'
wd=os.getcwd()   
file_name='registered_stocks_TICKER'+month+'-'+year+'.csv'
datafold='\\Data\\Chile\\'

df=CL.read_data(file_name,datafold)
get_fillings(month,year,df)
#receives month, year and dataframe with list of companies (must have Rut and File Type)

#file_name='CUPRUM'+'_'+month+'-'+year+'.pdf'
#datafold=wd+'\\Data\\Chile\\'+month+'-'+year+'\\'
#read_pdf_fil(file_name,datafold)




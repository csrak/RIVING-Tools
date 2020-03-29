import requests
import lxml.html as lh
import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
from pathlib import Path
from difflib import SequenceMatcher
import re
import tabula #install tabula-py
import PyPDF2
import Chile_Data as CL
from zipfile import ZipFile



##
##
# Format_Data -> Obtains and formats data already downloaded from updated databases and creates joint database
#  ###### USE ONLY IF UPDATE DATA THROWS NO ERRORS  ##############
####################################################################

def Format_Data(month, year):

    #Reads files downloaded from databases

    file_name='registered_stocks_'+month+'-'+year+'.csv'
    datafold='/Data/Chile/'
    ruts=CL.read_data(file_name,datafold)

    file_name='registered_stocks_mw.csv'
    tick=CL.read_data(file_name,datafold)

    # Then compares both to select right tickers 
    ruts=CL.Tick2Rut(ruts,tick) #Not  used anymore, since compares names manually

    # Finally saves results 
    wd=os.getcwd()
    file_name='registered_stocks_TICKER'+month+'-'+year+'.csv'
    datafold=wd+'/Data/Chile/'
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
    Format_Data(month, year)
    return 0

def get_fillings(month,year,df): 
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
            fnames[i]=month+'-'+year+'/'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.zip'
        elif filet==0:
            fnames[i]=month+'-'+year+'/'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.pdf'
        

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
    
def upandgetem(): # Updates tickers, list of companies and then downloads the fillings for the right month and year
    month='06'  # 03 06 09 12
    year='2019' # any
    wd=os.getcwd()   

    Update_Data(month,year)
    file_name='registered_stocks_TICKER'+month+'-'+year+'.csv'
    datafold='/Data/Chile/'

    df=CL.read_data(file_name,datafold)
    get_fillings(month,year,df)

def read_spec_xblr(param, param2,atparam2, folder):
    # If returns -1 it means not found
    # read_spec_xlbr searches value of solicited paremeter "param" in provided xblr filling folder
    # Second optional atribute can be put into param2, if not needed put 0
    # Same as read_xlbr but user selects specific parameter that wants to be retreived from filling
    #"param2" is a second condition to be a part of a certain atribute "atparam2", both are strings
    result=-1
    if param2==0: # if param2 is 0 we don't use it 
        os.chdir(folder)
        for file in glob.glob("*.xbrl"):
            with open(file, "r") as f:
                filling = f.read()
                
                soup = BeautifulSoup(filling, 'lxml')
                tag_list = soup.find_all()

                
                #Attributes are in lowercase, which is why this may either be done manually
                # or it may be done for all attributes 
                #Parameter to be searched
                for tag in tag_list:
                    if param == tag.name:
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        result = float(tag.text)
    else:
        os.chdir(folder)
        for file in glob.glob("*.xbrl"):
            with open(file, "r") as f:
                filling = f.read()
                
                soup = BeautifulSoup(filling, 'lxml')
                tag_list = soup.find_all()

                
                #Attributes are in lowercase, which is why this may either be done manually
                # or it may be done for all attributes 
                for tag in tag_list:
                    if param == tag.name and tag[atparam2]==param2:
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        result = float(tag.text)
    return result


def read_xblr(folder):
    
    #Parameters to extract

    # All fo them consist of a number and two codes
    # For example for Revenue
    # revenue[0] = Amount of  Revenue
    # revenue[1] = 0, 1 or 2
    #              0 means quarterly revenue
    #              1 means accumulated revenue
    #              2 means  revenue not found
    # revenue[2] = 0, 1
    #            0 means  CLP
    #            1 means  USD


    revenue=[0.0, 0, 0]


    os.chdir(folder)
    for file in glob.glob("*.xbrl"):
        with open(file, "r") as f:
            filling = f.read()
            
            soup = BeautifulSoup(filling, 'lxml')
            tag_list = soup.find_all()

            
            #Attributes are in lowercase, which is why this may either be done manually
            # or it may be done for all attributes 

            #Revenue
            found = 0 
            for tag in tag_list:
                if 'ifrs-full:revenue' == tag.name and tag['contextref']=='TrimestreActual':
                    #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                    revenue[0] = float(tag.text)
                    if (tag['unitref']=='CLP'):
                        revenue[2] = 0
                    else:
                        revenue[2] = 1
                    found = 1
            if found == 0:
                for tag in tag_list:
                    if 'ifrs-full:revenue' == tag.name and tag['contextref']=='TrimestreAcumuladoActual':
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        if (tag['unitref']=='CLP'):
                            revenue[2] = 0
                        else:
                            revenue[2] = 1
                        revenue[0] = float(tag.text)
                        found = 1
            if found == 1:
                revenue[1]=1
            else:
                revenue[1]=2
            print('\n revenue is '+ 'CLP')
            print(revenue)


wd=os.getcwd()   
datafold='/Data/Chile/'
read_xblr(wd+datafold+'06-2019/LASCONDES_06-2019/')


#receives month, year and dataframe with list of companies (must have Rut and File Type)

#file_name='CUPRUM'+'_'+month+'-'+year+'.pdf'
#datafold=wd+'/Data/Chile/'+month+'-'+year+'/'
#read_pdf_fil(file_name,datafold)




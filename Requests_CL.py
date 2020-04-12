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
import live_data as live
from zipfile import ZipFile





#We define hardcoded sets and lists that we will need to find the right values when parsing the fillings

months=['03','06','09','12'] #Possible months
years=range(2000,2100) # years are hardcoded for no good reason, just because
years=[str(i) for i in years]
years=set(years) #Set for faster search

present_trimester_tags={'TrimestreActual','p6','id113','Actual','ID_P3','CierreTrimestreActual','p1_Instant','id349'} #Only present trimester
weirder_tags= {'ctx_instant_Fecha_20190331','ctx_instant_Fecha_20190630','ctx_instant_Fecha_20190930','ctx_instant_Fecha_20191231','ID_P1'} # Present trimester, but may be wrong
cumulative_tags={'TrimestreAcumuladoActual','p1_Duration','id11792','AcumuladoAnoActual','id11','id15027'} #YearToDate
CLP_currency_tags={'CLP','id14'} #Tags to identify chilean currency

#Numbers
lista=[['Revenue','ifrs-full:revenue'],
['Net Profit','ifrs-full:ProfitLoss'],
['Operating Profit','ifrs-full:profitlossfromcontinuingoperations'],
# Equity and Liabilites
['Cash','ifrs-full:CashAndCashEquivalents'],
['Current Assets','ifrs-full:CurrentAssets'],
['Non-Current Assets','ifrs-full:NoncurrentAssets'],
['Goodwill','ifrs-full:Goodwill'],
['Intangible Assets','ifrs-full:IntangibleAssetsOtherThanGoodwill'],
['Assets','ifrs-full:Assets'], #Banks may have Assets > Current + Noncurrent, since banking assets are separated
['Current Liabilities','ifrs-full:CurrentLiabilities'],
['Equity','ifrs-full:EquityAttributableToOwnersOfParent'],
['Shares','ifrs-full:NumberOfSharesOutstanding'],
# Secondary Equity and Liabilites
['Inventories','ifrs-full:Inventories'],
['Shares Authorized','ifrs-full:NumberOfSharesAuthorised'],
#Cash Flows
['Net Operating Cashflows','ifrs-full:CashFlowsFromUsedInOperatingActivities'],
['Net Investing Cashflows','ifrs-full:CashFlowsFromUsedInInvestingActivities'],
['Net Financing Cashflows','ifrs-full:CashFlowsFromUsedInFinancingActivities'],
['Bank: Non-Banking investing cashflow','cl-hb:SubtotalFlujosEfectivoNetosProcedentesUtilizadosActividadesInversionNegociosNoBancarios'],
['Bank: Banking investing cashflow','cl-hb:SubtotalFlujosEfectivoNetosProcedentesUtilizadosActividadesInversionServiciosBancarios'],
#Secondary Items
['Payment for supplies','ifrs-full:PaymentsToSuppliersForGoodsAndServices'],
['Payment for employees','ifrs-full:PaymentsToAndOnBehalfOfEmployees'],
['Property sales (operating)','ifrs-full:ProceedsFromSalesOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities'],
['Dividends Paid','ifrs-full:DividendsPaidClassifiedAsFinancingActivities'],
['Forex','ifrs-full:EffectOfExchangeRateChangesOnCashAndCashEquivalents'],
['Trade receivables','ifrs-full:CurrentTradeReceivables'],
['Prepayments','ifrs-full:CurrentPrepayments'],
['Cash on hands','ifrs-full:CashOnHands'],
['Cash on banks','ifrs-full:BalancesWithBanks'],
['Cash short investment','ifrs-full:ShorttermInvestmentsClassifiedAsCashEquivalents']
]
##
##
# Format_Data -> Obtains and formats data already downloaded from updated databases and creates joint database
#  ###### USE ONLY IF UPDATE DATA THROWS NO ERROR  ##############
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

def get_fillings(month,year,df,datafold,wd): 
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
        print('Scraping '+a[i]+' Please wait...')
        temp=CL.scrap_file_links(a[i],filet)
        flinks.append(temp[0])
        filet=temp[1]
        if  filet==1:# file type XBLR
            fnames[i]=month+'-'+year+'/'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.zip'
        elif filet==0:
            fnames[i]=month+'-'+year+'/'+df.loc[i,'Ticker']+'_'+month+'-'+year+'.pdf'
        
    #wd=os.getcwd()
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
        temp=page.extractText()
        if 'PASIVOS Y PATRIMONIO NETO' in temp or 'PATRIMONIO Y PASIVOS' in temp:
            table = tabula.read_pdf(datafold+file_name,pages=i)
            print(table)
            ix=CL.getIndexes(table,'Pagos Anticipados')
            ix=CL.getIndexes(table,'Gastos Anticipados')
            print(ix)
        if 'ACTIVOS' in temp:
            table = tabula.read_pdf(datafold+file_name,pages=i)
            print(table)
            ix=CL.getIndexes(table,'Pagos Anticipados')
            ix=CL.getIndexes(table,'Gastos Anticipados')
            print(ix)
    #print(df.loc[3,'Pagos Anticipados'])
    
def upandgetem(month1,year1,month2 = 0,year2 = 0,scrap = 0 ):
    # Updates tickers, list of companies and then downloads the fillings for the right month and year
    # scrap can be changed to indicate you do not want to update ticker list, this will speed the process if you already have get the latest tickers
    # 
    #e.g.
    #month1='06'  # 03 06 09 12
    #year1='2019' # any

    #Optional month2 and year2
    #if used month1 and month2 is the starting date, and month2 year 2 is the last date to retrieve
    wd=os.getcwd()   

    if scrap == 0 :
        Update_Data(month1,year1)
    if month1 in months and year1 in years: # check if valid dates
        if month2 in months and year2 in years:
            for i in range(int(year1),int(year2)+1):
                year=str(int(year1)+i)
                for month in months:
                    if (year!=year1 or int(month1)>=int(month)) and (year!=year2 or int(month2)<=int(month)):
                        file_name='registered_stocks_TICKER'+month+'-'+year+'.csv'
                        datafold='/Data/Chile/'

                        df=CL.read_data(file_name,datafold)
                        get_fillings(month,year,df,datafold,wd)
        elif month2 ==0 and year2==0 :
            file_name='registered_stocks_TICKER'+month1+'-'+year1+'.csv'
            datafold='/Data/Chile/'
            df=CL.read_data(file_name,datafold)
            get_fillings(month1,year1,df,datafold,wd)
        else:
           print('Invalid Date 2') 
    else:
        print('Invalid Date 1')
        

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
                    if param == tag.name and (param2 in tag[atparam2]):
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        result = float(tag.text)
    return result


def read_xblr(folder,fin_dat_list):
    
    for i in range(0,len(fin_dat_list)):
        for j in range(0,len(fin_dat_list[i])):
            fin_dat_list[i][j]=fin_dat_list[i][j].lower()
    #fin_dat_list has the names of the parameters to extract

    # All fo them consist of a number and two codes
    # For example for Revenue
    # fin_dat[0] = String with name of data
    # fin_dat[1] = Amount of  Revenue
    # fin_dat[2] = 0, 1 or 2
    #              0 means quarterly revenue
    #              1 means accumulated revenue
    #              2 means  revenue not found
    # fin_dat[3] = 0, 1
    #            0 means  CLP
    #            1 means  USD


    #revenue=[0.0, 0, 0]
    final_list=[]

    os.chdir(folder)
    for file in glob.glob("*.xbrl"):
        with open(file, "r", errors='ignore') as f:
            filling = f.read()
            
            soup = BeautifulSoup(filling, 'lxml')
            tag_list = soup.find_all()

            
            #Attributes are in lowercase, which is why this may either be done manually
            # or it may be done for all attributes 

            #Revenue
            fin_dat=['', 0.0, 0, 0]
            for i in range (0,len(fin_dat_list)):
                fin_dat=['', 0.0, 0, 0]
                found = 0 # Some very funny guys at some companies think it makes sense to use dates for reference numbers in standarized fillings
                                #So I'm hardcoding tags, which is horrible
                for tag in tag_list:
                    if fin_dat_list[i][1] == tag.name and (tag['contextref'] in present_trimester_tags):
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        fin_dat[1] = float(tag.text)   
                        if (tag['unitref'] in CLP_currency_tags):
                            fin_dat[3] = 0
                        else:
                            fin_dat[3] = 1
                        found = 1
                        fin_dat[2]=0
                if found == 0:
                    for tag in tag_list:
                        if fin_dat_list[i][1] == tag.name and (tag['contextref'] in weirder_tags):
                            #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                            if (tag['unitref'] in CLP_currency_tags):
                                fin_dat[3] = 0
                            else:
                                fin_dat[3] = 1
                            fin_dat[1] = float(tag.text)
                            found = 1
                            fin_dat[2]=0
                if found == 0:
                    for tag in tag_list:
                        if fin_dat_list[i][1] == tag.name and (tag['contextref'] in cumulative_tags):
                            #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                            if (tag['unitref'] in CLP_currency_tags):
                                fin_dat[3] = 0
                            else:
                                fin_dat[3] = 1
                            fin_dat[1] = float(tag.text)                            
                            found = 1
                            fin_dat[2]=1
                if found == 0:
                    fin_dat[2]=2
                fin_dat[0]=fin_dat_list[i][0]
                final_list.append(fin_dat)
                #print('\n' + fin_dat[0] +' is '+ 'CLP')
                #print(fin_dat)
    temp_final_list=[]
    for j in range(0,4):
        dim=[]
        for i in range(0,len(final_list)):
            dim.append(final_list[i][j])
        temp_final_list.append(dim)
    final_list=temp_final_list
    column_names = final_list.pop(0)
    fin_df = pd.DataFrame(final_list, columns=column_names)
    return fin_df


#receives month, year and dataframe with list of companies (must have Rut and File Type)

#file_name='CUPRUM'+'_'+month+'-'+year+'.pdf'
#datafold=wd+'/Data/Chile/'+month+'-'+year+'/'
#read_pdf_fil(file_name,datafold)
####
def test_xblr(param, param2,atparam2, folder):
    # function for testing changes in xblr parsing functions
    result=0.0
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
                    #print(tag[atparam2])
                    if param == tag.name and (param2 in tag[atparam2]) and ('InfoSegmOper' in tag[atparam2]):
                        #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                        result += float(tag.text)
    return result





#Useful Data
#list2=[['Major Customer Risk','ifrs-full:DisclosureOfSegmentsMajorCustomersExplanatory'],]

def all_companies(lista,folder,month,year):

    ## The result will be a dataframe of all data with indicated date informat int -> year*100+month
    #  and stock ticker in "Ticker" column   
    #

    #datafold='/Data/Chile/'
    #file_name='registered_stocks_mw.csv'
    #tick=CL.read_data(file_name,datafold)
    #stocks=[[i,[],[]] for i in tick.loc[:,"Ticker"]]
    #print(stocks)
    all_stocks_all_dates = []
    path=r""+folder
    subfolders = [f.path for f in os.scandir(path) if f.is_dir()]
    subfolders2 = [i.replace(folder,'') for i in subfolders] #To compare names, easier for selecting right folders
    subfolders = [subfolders[i] for i in range(0,len(subfolders)) if len(subfolders2[i])==7] # We select only folders with 7 letters (nothing happens if wrong folder so it is a soft condition)
    print('Found these folders:')
    print(*subfolders2, sep = "\n")
    temp=[]   #Aux for selected subfolders
    temp2=[]  #Aux for dates of selected subfolders
    for i in range(0,len(subfolders)):
        if (int(subfolders2[i][3:7])>int(year) or (int(subfolders2[i][0:2])>=int(month) and subfolders2[i][3:7] == year)): # We select desired dates
            temp.append(subfolders[i])
            temp2.append(int(subfolders2[i][3:7])*100+int(subfolders2[i][0:2]))
    subfolders=temp
    subfolders2=temp2
    print('Selected Dates:')
    print(*subfolders2, sep = "\n")
    subfolders= tuple(zip(subfolders, subfolders2)) # Convert to tuple to sue numbers
    #subfolders=sorted(subfolders,key = lambda x: x[1]) #Sort, not used anymore
    for i in range(0,len(subfolders)): #Now we go into each selected folder searching for folders with stock data
        #for s in stocks:
        #     s[1].append(subfolders[i][1])
        # Not used anymore
        path=r""+subfolders[i][0]
        stockfolders = [f.path for f in os.scandir(path) if f.is_dir()]
        for j in range(0,len(stockfolders)): #Now for each folder with stock data we get requested data
            #print(stockfolders[j])
            listafinal=read_xblr(r""+stockfolders[j]+'/',lista)
            ticker=stockfolders[j].replace(subfolders[i][0],'')
            ticker=ticker[1:-8]
            print('\n ' + ticker + ' Found for date: ' + str(subfolders[i][1])[0:4] + ' / ' + str(subfolders[i][1])[4:6])
            listafinal['Date']=subfolders[i][1]
            listafinal['TICKER']=ticker
            if (j==0):
                print(listafinal)
            all_stocks_all_dates.append(listafinal)

    all_stocks_all_dates = pd.concat(all_stocks_all_dates)
    file_name='Database_Chile_Since_'+month+'-'+year+'.csv'
    all_stocks_all_dates.to_csv(folder+file_name, index = None, header=True)
    print(all_stocks_all_dates)
    

#upandgetem('06','2018',scrap=1)
#upandgetem('03','2019')
#upandgetem('09','2019')
#upandgetem('12','2019')
#upandgetem('12','2018')
#upandgetem('09','2018')
#upandgetem('03','2020')
#wd=os.getcwd()   
#datafold='/Data/Chile/'
#all_companies(lista,wd+datafold,'03','2018')
#listafinal=read_xblr(wd+datafold+'06-2019/LASCONDES_06-2019/',lista)
#res=test_xblr('ifrs-full:profitlossfromcontinuingoperations','_ACT','contextref',wd+datafold+'06-2019/FALABELLA_06-2019/')
#print(res)
#print(listafinal)


#read_pdf_fil('SECURITY_12-2019.pdf',wd+datafold+'12-2019NOTYET/')
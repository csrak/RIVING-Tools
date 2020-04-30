import requests
import lxml.html as lh
import pandas as pd
import os
import glob
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path
from difflib import SequenceMatcher
import re
import tabula #install tabula-py
import PyPDF2
import Chile_Data as CL
#import live_data as live
from zipfile import ZipFile
import fnmatch
import time





#We define hardcoded sets and lists that we will need to find the right values when parsing the fillings

months=['03','06','09','12'] #Possible months
years=range(2000,2100) # years are hardcoded for no good reason, just because
years=[str(i) for i in years]
years=set(years) #Set for faster search

present_trimester_tags={'TrimestreActual','p6','id113','Actual','ID_P3','CierreTrimestreActual','p1_Instant'}#,'id349','id3810'} #Only present trimester
#weirder_tags= {'ctx_instant_Fecha_20190331','ctx_instant_Fecha_20190630','ctx_instant_Fecha_20190930','ctx_instant_Fecha_20191231','ID_P1'} # Present trimester, but may be wrong
cumulative_tags={'TrimestreAcumuladoActual','p1_Duration','id11792','AcumuladoAnoActual'}#,'id11','id15027'} #YearToDate
CLP_currency_tags={'CLP','id14','shares','pure'} #Tags to identify chilean currency

#Numbers
lista=[['Revenue','ifrs-full:revenue'],
['Net Profit','ifrs-full:ProfitLoss'],
['Operating Profit','ifrs-full:profitlossfromcontinuingoperations'],
['Interest Revenue','ifrs-full:InterestRevenueExpense'],
['Cash from sales','ifrs-full:ReceiptsFromSalesOfGoodsAndRenderingOfServices'],
['Cash from yield','ifrs-full:ReceiptsFromPremiumsAndClaimsAnnuitiesAndOtherPolicyBenefits'],
['Cash from rent','ifrs-full:ReceiptsFromRentsAndSubsequentSalesOfSuchAssets'],
['Cash to payments','ifrs-full:PaymentsToSuppliersForGoodsAndServices'],
['Cash to other payments','ifrs-full:OtherCashPaymentsFromOperatingActivities'],
['Speculation Cash','ifrs-full:CashReceiptsFromFutureContractsForwardContractsOptionContractsAndSwapContractsClassifiedAsInvestingActivities'],
['Current payables','ifrs-full:TradeAndOtherCurrentPayables'],
['Cost of Sales','ifrs-full:CostOfSales'],
['EBIT','ifrs-full:ProfitLossBeforeTax'],
['Depreciation','ifrs-full:DepreciationAndAmortisationExpense'],
['Interest','ifrs-full:InterestExpense'],
# Equity and Liabilites
['Cash','ifrs-full:CashAndCashEquivalents'],
['Current Assets','ifrs-full:CurrentAssets'],
['Liabilities','ifrs-full:Liabilities'],
['Marketable Securities','ifrs-full:OtherCurrentFinancialAssets'],
['Current Other Assets','ifrs-full:OtherCurrentNonfinancialAssets'],
['Provisions for Employees','ifrs-full:CurrentProvisionsForEmployeeBenefits'],
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
['Payment to employees','ifrs-full:PaymentsToAndOnBehalfOfEmployees'],
['Property sales (operating)','ifrs-full:ProceedsFromSalesOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities'],
['Dividends Paid','ifrs-full:DividendsPaidClassifiedAsFinancingActivities'],
['Forex','ifrs-full:EffectOfExchangeRateChangesOnCashAndCashEquivalents'],
['Trade receivables','ifrs-full:CurrentTradeReceivables'],
['Prepayments','ifrs-full:CurrentPrepayments'],
['Cash on hands','ifrs-full:CashOnHands'],
['Cash on banks','ifrs-full:BalancesWithBanks'],
['Cash short investment','ifrs-full:ShorttermInvestmentsClassifiedAsCashEquivalents'],
['Employee Benefits','ifrs-full:EmployeeBenefitsExpense']
#Disclosures (Non Numerical)
#['Disclosure: Intangible','ifrs-full:DisclosureOfIntangibleAssetsExplanatory'],
#['Disclosure: Leases','ifrs-full:DisclosureOfLeasesExplanatory']
]


lista_instant=[['Cash','ifrs-full:CashAndCashEquivalents'],
['Liabilities','ifrs-full:Liabilities'],
['Current Assets','ifrs-full:CurrentAssets'],
['Non-Current Assets','ifrs-full:NoncurrentAssets'],
['Goodwill','ifrs-full:Goodwill'],
['Intangible Assets','ifrs-full:IntangibleAssetsOtherThanGoodwill'],
['Assets','ifrs-full:Assets'], #Banks may have Assets > Current + Noncurrent, since banking assets are separated
['Current Liabilities','ifrs-full:CurrentLiabilities'],
['Equity','ifrs-full:EquityAttributableToOwnersOfParent'],
['Shares','ifrs-full:NumberOfSharesOutstanding'],
['Inventories','ifrs-full:Inventories'],
['Shares Authorized','ifrs-full:NumberOfSharesAuthorised'],
['Cash on hands','ifrs-full:CashOnHands'],
['Cash on banks','ifrs-full:BalancesWithBanks'],
['Cash short investment','ifrs-full:ShorttermInvestmentsClassifiedAsCashEquivalents'],
['Trade receivables','ifrs-full:CurrentTradeReceivables'],
['Liabilities','ifrs-full:Liabilities'],
['Provisions for Employees','ifrs-full:CurrentProvisionsForEmployeeBenefits'],
['Marketable Securities','ifrs-full:OtherCurrentFinancialAssets'],
['Current Other Assets','ifrs-full:OtherCurrentNonfinancialAssets'],
]
##
##
# Format_Data -> Obtains and formats data already downloaded from updated databases and creates joint database
#  ###### USE ONLY IF UPDATE DATA THROWS NO ERROR  ##############
####################################################################
lista_instant=[a[0].lower() for a in lista_instant]

def check_parameter(param):
    if param.lower() not in [a[0].lower() for a in lista]:
        print('Current available parameters:\n')
        print(*lista, sep='\n')
        raise SystemExit('Please select a parameter from the available database') 
    else:
        return param.lower()


def Format_Data(month, year):

    #Reads files downloaded from databases

    file_name='Ticker_data/registered_stocks_'+month+'-'+year+'.csv'
    datafold='/Data/Chile/'
    ruts=CL.read_data(file_name,datafold)

    file_name='registered_stocks.csv'
    tick=CL.read_data(file_name,datafold)

    # Then compares both to select right tickers 
    ruts=CL.Tick2Rut(ruts,tick) #Not  used anymore, since compares names manually

    # Finally saves results 
    wd=os.getcwd()
    file_name='Ticker_data/registered_stocks_TICKER'+month+'-'+year+'.csv'
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
    #CL.scrap_mw() #Online option, not up to dateas of 14/04/2020, so not recommended
    CL.scrap_offline()
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
            file_name='Ticker_data/registered_stocks_TICKER'+month1+'-'+year1+'.csv'
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

def get_unknown_reference(soup,param,month,year):
    #Receives the name of a parameter from the list of parameters, a mont, a year, and a parsed xblr file
    #It searches for the data in the file trying to search trimestral data first
    #If trimestral data not found it returns YTD data
    #os.chdir(folder)
    acum=2  #2 means not found (default) 1 means YTD data found, 0 means trimestral data found
    end_value=np.nan #Value of data, nan if not found
    clp=0 #Currency, CLP is 0, USD is 1, no other currencies implemented yet
    start_month=str(int(month)-2) #For searching the right period calculates the start of the trimester (If trimester  is different it won't find it)
    if int(start_month)<10:
        start_month='0'+start_month #Strings usually include the 0
    contexts=soup.find_all('xbrli:context')
    filling_type=soup.find_all(param)
    #print(filling_type)
    if not filling_type:
        param=param.replace('ifrs-full','ifrs')
    per='period'
    endd='enddate'
    stard='startdate'
    inst='instant'
    scen='scenario'
    if contexts:
        per='xbrli:'+per
        endd='xbrli:'+endd
        stard='xbrli:'+stard
        inst='xbrli:'+inst
        scen='xbrli:'+scen
    else:
        contexts=soup.find_all('context')
    tag_list = soup.find_all(param)
    #print(param + '=' )
    #print(tag_list)
    #ref_list=[tag['contextref'] for tag in tag_list]
    #print(tag_list)
    for datafromperiods in tag_list: #Loop through the different periods for which the data exists
        for context in contexts: #Loop through different defined periods
            if (context.find(scen)) == None:
                if datafromperiods['contextref']==context['id']: #Identifies the correspondent period from the defined ones
                    #print(((context.find('xbrli:period')).find('xbrli:enddate')).contents)
                    if ((context.find(per)).find(inst)) != None: #If instant tag exists means there is no period (I.E.For current assets)
                        if (datafromperiods['unitref'] in CLP_currency_tags):  # Searches for currency code in list
                                clp = 0
                        else:
                                clp = 1
                            #print('Found Trimestral')
                        return datafromperiods.text,0.0,clp  #If instant means there are no existent (useful) periods so we return this data
                    if year+'-'+month in ((context.find(per)).find(endd)).contents[0]:                            
                        if year+'-'+start_month in ((context.find(per)).find(stard)).contents[0]:
                            if (datafromperiods['unitref'] in CLP_currency_tags):# Searches for currency code in list
                                clp = 0
                            else:
                                clp = 1
                            #print('Found Trimestral')
                            return datafromperiods.text,0,clp #If trimestral data found, search stops and we can go back
                        elif year+'-01' in ((context.find(per)).find(stard)).contents[0]:
                            if (datafromperiods['unitref'] in CLP_currency_tags):# Searches for currency code in list
                                clp = 0
                            else:
                                clp = 1               
                            acum=1 #If we find YTD data we save it, but keep searching for the trimestral data
                            end_value=datafromperiods.text #If we find YTD data we save it, but keep searching for the trimestral data
                            #print('Found Accumulated')
    return end_value, acum,clp

    
def read_xblr(folder,fin_dat_list,month,year):
    start_time=time.time()
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

    name="*.xbrl"
    if len(fnmatch.filter(os.listdir(folder), '*.xbrl'))>1: #For now if they report consolidated and individual files we consider only the consoloidated, this may and should be changed in the future as an option
        name="*_C.xbrl"    
    os.chdir(folder)
    for file in glob.glob(name): #usually there is only one in the folder, but didn't want to code a name generator so we do it for "all" which is more like "any"
        with open(file, "r", errors='ignore') as f:

            filling = f.read()
            
            soup = BeautifulSoup(filling, 'lxml')
            tag_list = soup.find_all()
            #Attributes are in lowercase, which is why this may either be done manually
            # or it may be done for all attributes 
            #print(tag_list)
            fin_dat=['', 0.0, 0, 0]
            for i in range (0,len(fin_dat_list)):
                fin_dat=['', 0.0, 0, 0]
                found = 0 # Some very funny guys at some companies think it makes sense to use dates for reference numbers in standarized fillings
                                #So I'm hardcoding tags, which is horrible
                for tag in tag_list:
                    #print(tag.name)
                    #try:
                    #    print(tag["contextref"])
                    #except KeyError:
                    #    print('\n')
                    if fin_dat_list[i][1] == tag.name and (tag['contextref'] in present_trimester_tags):
                        #print(tag.name + ' '+ tag.text + ' ' + tag['unitref'] )
                        fin_dat[1] = float(tag.text)   
                        if (tag['unitref'] in CLP_currency_tags):
                            fin_dat[3] = 0
                        else:
                            fin_dat[3] = 1
                        found = 1
                        fin_dat[2]=0
                #if found == 0:
                #    for tag in tag_list:
                #        if fin_dat_list[i][1] == tag.name and (tag['contextref'] in weirder_tags):
                #            #print('Revenue' + ' '+ tag.text + ' ' + tag['unitref'] )
                #            if (tag['unitref'] in CLP_currency_tags):
                #                fin_dat[3] = 0
                #            else:
                #                fin_dat[3] = 1
                #            fin_dat[1] = float(tag.text)
                #            found = 1
                #            fin_dat[2]=0
                if found == 0:
                    for tag in tag_list:
                        if fin_dat_list[i][1] == tag.name and (tag['contextref'] in cumulative_tags):
                            #print( tag.name + ' '+ tag.text + ' ' + tag['unitref'] )
                            if (tag['unitref'] in CLP_currency_tags):
                                fin_dat[3] = 0
                            else:
                                fin_dat[3] = 1
                            fin_dat[1] = float(tag.text)                            
                            found = 1
                            fin_dat[2]=1
                if found == 0:#last resort
                    fin_dat[1],fin_dat[2],fin_dat[3]=get_unknown_reference(soup,fin_dat_list[i][1],month,year) #Should always find the right one, but it is also slower
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
    #print(fin_df)
    end = time.time()
    print('Execution time = ')
    print(end - start_time)
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

def all_companies(lista,folder,month,year,update=0,monthup=0,yearup=0):
    #Option Update, to update existing database with new data change update option to 1 and add a month and a year from an existing database file
    ## The result will be a dataframe of all data with indicated date informat int -> year*100+month
    #  and stock ticker in "Ticker" column   
    #

    #datafold='/Data/Chile/'
    #file_name='Ticker_data/registered_stocks_mw.csv'
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
    check=0
    #subfolders=sorted(subfolders,key = lambda x: x[1]) #Sort, not used anymore
    for i in range(0,len(subfolders)): #Now we go into each selected folder searching for folders with stock data
        #for s in stocks:
        #     s[1].append(subfolders[i][1])
        # Not used anymore
        path=r""+subfolders[i][0]
        stockfolders = [f.path for f in os.scandir(path) if f.is_dir()]
        for j in range(0,len(stockfolders)): #Now for each folder with stock data we get requested data
            #print(stockfolders[j])
            listafinal=read_xblr(r""+stockfolders[j]+'/',lista,str(subfolders[i][1])[4:6],str(subfolders[i][1])[0:4]) #Main function, we seach for all the aprametersfor the database in the folder
            ticker=stockfolders[j].replace(subfolders[i][0],'')
            ticker=ticker[1:-8]
            print('\n ' + ticker + ' Found for date: ' + str(subfolders[i][1])[0:4] + ' / ' + str(subfolders[i][1])[4:6])
            listafinal['Date']=subfolders[i][1]
            listafinal['TICKER']=ticker            
            if (j==0):
                print('initial lenght = '+ str(len(listafinal.columns)))
                check=len(listafinal.columns)
                print(listafinal)
            if (len(listafinal.columns)!=check):
                print('weird lenght = '+ str(len(listafinal.columns)))
                print('\nNon Existing Company?')
                print(listafinal)
            else:
                all_stocks_all_dates.append(listafinal)

    all_stocks_all_dates = pd.concat(all_stocks_all_dates)
    if update==0:
        file_name='Database_Chile_Since_'+month+'-'+year+'.csv'
        all_stocks_all_dates.to_csv(folder+file_name, index = None, header=True)
        print(all_stocks_all_dates)
    else:
        try:
            file_name='Database_Chile_Since_'+monthup+'-'+yearup+'.csv'
            all_stocks_all_dates.to_csv(folder+file_name, mode = 'a', index = None, header=False)
        except IOError:
            print('Database file '+ file_name + 'does not exist')





#Update_Data('03','2013')
#upandgetem('03','2013')
#upandgetem('06','2013')
#upandgetem('09','2013')
#upandgetem('12','2013')
#upandgetem('03','2014')
#upandgetem('06','2014')
#upandgetem('09','2014')
#upandgetem('12','2014')
#upandgetem('03','2015')
#upandgetem('06','2015')
#upandgetem('09','2015')
#upandgetem('12','2015')
##upandgetem('03','2016')
#upandgetem('06','2016')
#upandgetem('09','2016')
#upandgetem('12','2016')
#upandgetem('03','2017')
#upandgetem('06','2017')
#upandgetem('09','2017')
#upandgetem('12','2017')
#upandgetem('03','2018')
#upandgetem('06','2018')
#upandgetem('09','2018')
#upandgetem('12','2018')
#upandgetem('03','2019')
#upandgetem('06','2019')
#upandgetem('09','2019')
#upandgetem('12','2019')
#upandgetem('03','2020')
#wd=os.getcwd()   
#datafold='/Data/Chile/'
#all_companies(lista,wd+datafold,'03','2013')
#read_xblr(wd+datafold+'03-2019/ATSA_03-2019/',lista,'03','2019')
#print(res)
#print(listafinal)
################

#read_pdf_fil('SECURITY_12-2019.pdf',wd+datafold+'12-2019NOTYET/')
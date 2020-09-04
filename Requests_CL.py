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

#Numbers
lista=[['Revenue','ifrs-full:revenue'],
['Net Profit','ifrs-full:ProfitLoss'],
['Operating Profit','ifrs-full:profitlossfromcontinuingoperations'],
['Non-Controlling Profit','ifrs-full:ProfitLossAttributableToNoncontrollingInterests'],
['EPS','ifrs-full:BasicAndDilutedEarningsLossPerShare'],
['Operating-EPS','ifrs-full:BasicAndDilutedEarningsLossPerShareFromContinuingOperations'],
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
    
def upandgetem(month1,year1,month2 = 0,year2 = 0,update = 0 ):
    # Updates tickers, list of companies and then downloads the fillings for the right month and year
    # update can be changed to indicate you do not want to update ticker list, this will speed the process if you already have get the latest tickers
    # 
    #e.g.
    #month1='06'  # 03 06 09 12
    #year1='2019' # any

    #Optional month2 and year2
    #if used month1 and month2 is the starting date, and month2 year 2 is the last date to retrieve
    wd=os.getcwd()   

    if update == 0 :
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

def get_unknown_reference(soup,currencies,param,month,year):
    #Receives the name of a parameter from the list of parameters, a mont, a year, and a parsed xblr file
    #It searches for the data in the file trying to search trimestral data first
    #If trimestral data not found it returns YTD data
    #os.chdir(folder)
    acum='12' #2 means not found (default) 1 means YTD data found, 0 means trimestral data found
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
                        if year+'-'+month in ((context.find(per)).find(inst)).contents[0]:
                            clp=currencies[datafromperiods['unitref']]
                                #print('Found Trimestral')
                            return datafromperiods.text,'10',clp  #If instant means there are no existent (useful) periods so we return this data
                        else:
                            continue
                    if year+'-'+month in ((context.find(per)).find(endd)).contents[0]:                            
                        if year+'-'+start_month in ((context.find(per)).find(stard)).contents[0]:
                            clp=currencies[datafromperiods['unitref']]
                            #print('Found Trimestral')
                            return datafromperiods.text,'10',clp #If trimestral data found, search stops and we can go back
                        elif year+'-01' in ((context.find(per)).find(stard)).contents[0]:
                            clp=currencies[datafromperiods['unitref']]           
                            acum=1 #If we find YTD data we save it, but keep searching for the trimestral data
                            end_value=datafromperiods.text #If we find YTD data we save it, but keep searching for the trimestral data
                            #print('Found Accumulated')
    if acum == 0:
        acum = '10'
    elif acum == 1:
        acum = '11'
    return end_value, acum,clp

def get_currencies(soup):
    #Receives the name of a parameter from the list of parameters, a mont, a year, and a parsed xblr file
    #It searches for the data in the file trying to search trimestral data first
    #If trimestral data not found it returns YTD data
    #os.chdir(folder)
    #For searching the right period calculates the start of the trimester (If trimester  is different it won't find it)
    param='xbrli:unit'
    units=soup.find_all(param)
    #print(filling_type)
    mes='measure'
    num='unitnumerator'
    div='divide'
    den='unitdenominator'
    if units:
        mes='xbrli:'+mes
        num='xbrli:'+num
        div='xbrli:'+div
        den='xbrli:'+den        
    else:
        units=soup.find_all('unit')
    #print(param + '=' )
    #print(tag_list)
    #ref_list=[tag['contextref'] for tag in tag_list]
    #print(tag_list)
    currency_list={}
    for unit in units: #Loop through the units
        if len(unit.find_all(mes))>1:
            #print(unit.find_all(mes))
            if 'usd' in ((((unit.find(div)).find(num)).find(mes)).contents[0]).lower():
                currency_list[unit['id']]= 1
            else:
                currency_list[unit['id']]= 0
        else:            
            #print(unit.find(mes).contents[0])
            if 'usd' in (unit.find(mes).contents[0]).lower():
                currency_list[unit['id']]= 1
            else:
                currency_list[unit['id']]= 0       
    #print(currency_list) 
    return currency_list



    
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
    # fin_dat[2] = q, a or n
    #              q means quarterly revenue
    #              a means accumulated revenue
    #              n means  revenue not found
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
            currencies=get_currencies(soup)
            tag_list = soup.find_all()
            #Attributes are in lowercase, which is why this may either be done manually
            # or it may be done for all attributes 
            #print(tag_list)
            fin_dat=['', 0.0, '12', 0]
            for i in range (0,len(fin_dat_list)):
                fin_dat=['', 0.0, '12', 0]
                found = 0 #Hardcoded msot common names for faster processing
                for tag in tag_list:
                    #print(tag.name)
                    #try:
                    #    print(tag["contextref"])
                    #except KeyError:
                    #    print('\n')
                    if fin_dat_list[i][1] == tag.name and (tag['contextref'] in present_trimester_tags):
                        #print(tag.name + ' '+ tag.text + ' ' + tag['unitref'] )
                        fin_dat[1] = float(tag.text)   
                        fin_dat[3] = currencies[tag['unitref']]
                        found = 1
                        fin_dat[2]='10'
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
                            fin_dat[3] = currencies[tag['unitref']]
                            fin_dat[1] = float(tag.text)                            
                            found = 1
                            fin_dat[2]='11'
                if found == 0:#last resort
                    fin_dat[1],fin_dat[2],fin_dat[3]=get_unknown_reference(soup,currencies,fin_dat_list[i][1],month,year) #Should always find the right one, but it is also slower
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

def all_companies(lista,folder,month,year,monthup=0,yearup=0,update=0,updatemonth=0,updateyear=0):
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
            file_name='Database_Chile_Since_'+updatemonth+'-'+updateyear+'.csv'
            df=CL.read_data(file_name,folder,1)
            all_stocks_all_dates=pd.concat([df, all_stocks_all_dates], ignore_index=True)
            all_stocks_all_dates.drop_duplicates(inplace = True,keep = 'last',subset = ['Date','TICKER','revenue'] ) 
            all_stocks_all_dates.to_csv(folder+file_name,index = None, header=True)
        except IOError:
            print('Database file '+ file_name + 'does not exist')
    #if monthup == 0 or yearup == 0:
    #    all_banks(folder+'Banks/',month,year)
    #else:
    #    all_banks(folder+'Banks/',month,year, monthup, yearup)
    


def read_bank_txt(datafold,filename,names,mb1,mr1,mc1,curr_type='total'):
    #With Option curr_type tells you can select which currency you want to consult (always expressed in CLP)
    # total is default, which is all currencies summed
    # uf is adjustable by inflation currency
    # ex1 means national currency but adjustable to foreigner exchange
    # ex2 means directly foreign currency
    #filename is year+month as strings
    #print(mb1)
    curr_type=curr_type.lower()
    f_mb1=[]
    f_mr1=[]
    f_mc1=[]
    for name in names:
        b1=[np.nan]*len(mb1)
        r1=[np.nan]*len(mr1)
        c1=[np.nan]*len(mc1)
        # First for Model B1
        ##############################################
        try:
            df=pd.read_csv(datafold+'b1'+filename+name+'.txt', skiprows=0 ,delimiter='\t',header=0,names=['code','clp', 'uf','ex1','ex2'] ,usecols=['code','clp', 'uf','ex1','ex2'],index_col=False,encoding ='latin1',decimal=',',dtype={'code':str,'clp': np.float64, 'uf': np.float64,'ex1': np.float64,'ex2': np.float64})
            cod_in_fill=df['code'].tolist()
            if curr_type=='total':
                clp_in_fill=df['clp'].tolist()
                uf_in_fill=df['uf'].tolist()
                ex1_in_fill=df['ex1'].tolist()
                ex2_in_fill=df['ex2'].tolist()
                selected=[(clp_in_fill[i]+uf_in_fill[i]+ex1_in_fill[i]+ex2_in_fill[i])*1000000 for i in range(len(cod_in_fill))] #The amounts are in millions of CLP in the files so we multiply
            else:
                try:
                    selected=df[curr_type].tolist()
                    selected=[select*1000000 for select in selected]
                except KeyError:
                    print('Please enter a valid curr_type (clp, uf, ex1, ex2) or leave the default for a calculation of the total')
                    print('------------------------------------------------------------------------------------------------------')
                    return    
            for i in range(len(selected)):
                try:
                    ix=mb1.index(cod_in_fill[i])
                    b1[ix]=selected[i]
                except ValueError:
                    print(str(cod_in_fill[i])+' Not Found')
                    continue
            f_mb1.append(b1)
        except FileNotFoundError:
            print('Corrupt filename (format problem):')
            print(datafold+'b1'+filename+name+'.txt')
        # Then for Model R1
        ##############################################
        try:
            df=pd.read_csv(datafold+'r1'+filename+name+'.txt', skiprows=0 ,delimiter='\t',header=0,names=['code','clp', 'uf','ex1','ex2'] ,usecols=['code','clp', 'uf','ex1','ex2'],index_col=False,encoding ='latin1',decimal=',',dtype={'code':str,'clp': np.float64, 'uf': np.float64,'ex1': np.float64,'ex2': np.float64})
            cod_in_fill=df['code'].tolist()
            if curr_type=='total':
                clp_in_fill=df['clp'].tolist()
                uf_in_fill=df['uf'].tolist()
                ex1_in_fill=df['ex1'].tolist()
                ex2_in_fill=df['ex2'].tolist()
                selected=[(clp_in_fill[i]+uf_in_fill[i]+ex1_in_fill[i]+ex2_in_fill[i])*1000000 for i in range(len(cod_in_fill))]
            else:
                try:
                    selected=df[curr_type].tolist()
                    selected=[select*1000000 for select in selected]
                except KeyError:
                    print('Please enter a valid curr_type (clp, uf, ex1, ex2) or leave the default for a calculation of the total')
                    print('------------------------------------------------------------------------------------------------------')
                    return    
            for i in range(len(selected)):
                try:
                    ix=mr1.index(cod_in_fill[i])
                    r1[ix]=selected[i]
                except ValueError:
                    #print(str(cod_in_fill[i])+' Not Found')
                    continue
            f_mr1.append(r1)
        except FileNotFoundError:
            print('Corrupt filename (formatting problem):')
            print(datafold+'r1'+filename+name+'.txt')
            # Then for Model C1
            ##############################################
        try:
            df=pd.read_csv(datafold+'c1'+filename+name+'.txt', skiprows=0 ,delimiter='\t',header=0,names=['code','total'] ,usecols=['code','total'],index_col=False,encoding ='latin1',decimal=',',dtype={'code':str,'total': np.float64})
            cod_in_fill=df['code'].tolist()
            if curr_type=='clp':
                curr_type='total'
            try:
                selected=df[curr_type].tolist()
                selected=[select*1000000 for select in selected]
            except KeyError:
                print('Model C1 not included due to being only in CLP')
                continue
            for i in range(len(selected)):
                try:
                    ix=mc1.index(cod_in_fill[i])
                    c1[ix]=selected[i]
                except ValueError:
                    #print(str(cod_in_fill[i])+' Not Found')
                    continue
            f_mc1.append(c1)
        except FileNotFoundError:
            print('Corrupt filename (formatting problem):')
            print(datafold+'c1'+filename+name+'.txt')
        #print(b1)
    return f_mb1,f_mr1,f_mc1
        
def duplicated_varnames(df):
    repeat_dict = {}
    var_list = list(df) # list of varnames as strings
    for varname in var_list:
        # make a list of all instances of that varname
        test_list = [v for v in var_list if v == varname] 
        # if more than one instance, report duplications in repeat_dict
        if len(test_list) > 1: 
            repeat_dict[varname] = len(test_list)
    return repeat_dict
    
def all_banks(folder,month,year,monthup='03',yearup='2020',update=0,ticktofile=0):
    os.chdir(folder)
    month=int(month)
    year=int(year)
    monthup=int(monthup)
    yearup=int(yearup)
    if not os.path.exists(folder+'Tickers/'):
        os.mkdir(folder+'Tickers/')
    previous_months=(yearup-year)*12-((month))+monthup+1 #We calculate how many months 
    print('months')
    print(previous_months)
    name_month=int(monthup)
    name_year=int(yearup)
    final_df=[]
    for i in range(previous_months):
        if name_month<10:
            name_month='0'+str(name_month)      
        filename=str(name_year)+str(name_month)
        if glob.glob(folder+'Banks_'+str(name_month)+'-'+str(name_year)+'/b1*'):
            datafold=folder+'Banks_'+str(name_month)+'-'+str(name_year)+'/'
            print('c1 %s',datafold)    
        else:
            try:
                datafold=glob.glob(folder+'Banks_'+str(name_month)+'-'+str(name_year)+'/'+'*')[0]+'/'
                print('c2 %s',datafold)   
            except IndexError:
                print(folder+'Banks_'+str(name_month)+'-'+str(name_year)+'/ '+' Folder not found')
                continue
        name_month=int(name_month)
        if name_month>1:
            name_month-=1
        else:
            name_month=12
            name_year-=1  
        names,mb1,mr1,mc1=CL.get_bank_tickers(datafold)
        repeated=['5001000','5002000','5100000','1270000','1800000','2100000'] #Fors some reason in odler files these are repeated and are basically nan filled columns
        for cd in repeated:
            try:
                ix=mr1[1].index(cd)
                mr1[0].pop(ix)
                mr1[1].pop(ix)
            except ValueError:
                pass
        if ticktofile!=0: #If the option to print the tickers to files is enabled we print monthly tickers
            df=df = pd.DataFrame(columns=['ticker','codes'])
            df['ticker']=names[0]
            df['codes']=names[1]
            df.to_csv(folder+'Tickers/'+'Tickers_'+filename, index = None, header=True)
        b1,r1,c1=read_bank_txt(datafold,filename,names[1],mb1[1],mr1[1],mc1[1])
        #Now we change the lit of lists to pandas dataframe, rearranging first to coincide iwth pandas formatting
        final_list=[]
        for i in range(len(b1)):
            c1[i].append(names[0][i])
            c1[i].append(filename)
            final_list.append(b1[i]+r1[i]+c1[i])
        df=pd.DataFrame(final_list,columns=mb1[1]+mr1[1]+mc1[1]+['ticker','date'])
        #df = df.loc[:,~df.columns.duplicated()]
        print(duplicated_varnames(df))
        if b1 or r1:
            print(filename)
            df=df.dropna(axis=1, how='all')            
            final_df.append(df)
    df1=pd.DataFrame()
    df1=df1.append(final_df)
    name_month=int(name_month)
    if name_month<10:
        name_month='0'+str(name_month) 
    df_col=pd.DataFrame(data={'balance':mb1[0]})
    df_col2=pd.DataFrame(data={'result':mr1[0]})
    df_col=pd.concat([df_col,df_col2], ignore_index=True, axis=1)
    df_col2=pd.DataFrame(data={'complementary':mc1[0]})  
    df_col=pd.concat([df_col,df_col2], ignore_index=True, axis=1)
    df_col.to_csv(folder+'bank_parameters_since_'+str(month)+'-'+str(year)+'.csv', index = None, header=True)
    df1.to_csv(folder+'bank_database_since_'+str(month)+'-'+str(year)+'.csv', index = None, header=True)

#update_database

#all_banks('/Data/Chile/Banks/','11','2012', '03', '2020')
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
#upandgetem('03','2016')
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
#upandgetem('06','2020')
#wd=os.getcwd()   
#datafold='/Data/Chile/'
#all_companies(lista,wd+datafold,'03','2013')
#all_companies(lista,wd+datafold,'06','2020',update=1,updatemonth='03',updateyear='2013')
#read_xblr(wd+datafold+'03-2019/AESGENER_03-2019/',lista,'03','2019')
#print(res)
#print(listafinal)
################

#read_pdf_fil('SECURITY_12-2019.pdf',wd+datafold+'12-2019NOTYET/')
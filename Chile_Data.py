#By Cristian

#Launch notes:
#   Important: Careful with debugging mode/low speed internet, this may cause for the cmf webpage to timeout and
#              larger files to not be downloaded even if they exist


import requests
import live_data as ld
import time
import glob
import lxml.html as lh
import pandas as pd
import os
from pathlib import Path
from difflib import SequenceMatcher
import re
from bs4 import BeautifulSoup
from zipfile import ZipFile
from zipfile import BadZipfile
import tabula #install tabula-py
import PyPDF2
import unidecode
from datetime import datetime
import shutil
###
#####
###### 
#    Functions for scraping data, general functions and some hard-code for specific scraping is found here
#       Specifically for importing data from chilean stocks, into csv file, mostly into ~/data/chile/ folder


MAXMONTHS=100 # Number maximum of empty urls in between monthly fillings, used to stop checking if user tries to obtain 
              # dates that do not exist

def getIndexes(dfObj, value):
    #''' Get index positions of value in dataframe i.e. dfObj.'''
    listOfPos = list()
    # Get bool dataframe with True at positions where the given value exists
    result = dfObj.isin([value])
    # Get list of columns that contains the value
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    # Iterate over list of columns and fetch the rows indexes where value exists
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append((row, col))
            # Return a list of tuples indicating the positions of value in the dataframe
    return listOfPos


def scrap_company_Links(companies_list,month, year):
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url='http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs='+month+'&aa_ifrs='+year
    page = requests.get(url, headers=agent) 
    page=lh.fromstring(page.content)

    #We obtain every <a and take the URLS to a list
    link = page.xpath('//a')
    out=['Not Found']*companies_list.shape[0]
    
        
    #Selected format is searched 0=PDF 1=XBLR 2=BOTH
    for link in link:
        for i in range (0,companies_list.shape[0]):
            rut=companies_list.loc[i,'Rut']
            if 'href' in link.attrib and rut in link.attrib['href']:
                out[i]=link.attrib['href']
                out[i]='http://www.cmfchile.cl/institucional/mercados/'+ out[i]               
            
                
    return out


def scrap_file_links(url,filet):
    if filet==999:
        return 'Invalid Link'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}
    try:
        page = requests.get(url, headers=agent) 
    except requests.exceptions.ConnectionError:
        print("Connection error -> Trying next one")    
        return 'Invalid Link'  
    except:
        print("Error:"+url+"\n  Trying By chunks")
        page = ld.get_url_bychunks(url)
    page=lh.fromstring(page.content)
    #We obtain every <a and take the URLS to a list
    link = page.xpath('//a')
    out=''
    for link in link:
        
    #Selected format is searched 0=PDF 1=XBLR 2=BOTH
        if  'href' in link.attrib and 'Estados financieros (XBRL)' in link.attrib['href']:
            out=link.attrib['href']
            out='http://www.cmfchile.cl/institucional'+ out[2:len(out)] 
            out=re.sub(' ','%20',out)
            filet=1
        elif 'href' in link.attrib and 'Estados financieros (PDF)' in link.attrib['href']:
            out=link.attrib['href']
            out='http://www.cmfchile.cl/institucional'+ out[2:len(out)]  
            out=re.sub(' ','%20',out)
            filet=0
        
      
    return out,filet

#print( scrap_File_Links('http://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut=96885880&rut_inc=&grupo=0&tipoentidad=RVEMI&vig=VI&row=AAAwy2ACTAAABzBAAN&mm=03&aa=2019&tipo=C&orig=lista&control=svs&tipo_norma=IFRS&pestania=3',0) )
#a=scrap_company_Links('96885880','03','2019')
#print(a)
#Download list not working/not used yet
def download_list(month,year):
    # www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs_excel2.php?aa=2019&mm=03
    template = 'http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs_excel2.php?'
    year='aa='+year
    month='&mm='+month
    url=template+year+month
    myfile = requests.get(url)
    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='Ticker_data/registered_stocks_'+month+'-'+year+'.xls'
    datafold='/Data/Chile/'
    open(wd+datafold+file_name, 'wb').write(myfile.content)





#Selects right url depending on scraping to do
def url_generator(site,month,year):
    url='no url found'
    if site=='cmf'or site=='cmf.cl':
        #http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs=03&aa_ifrs=2019 For first 3 months of 2019
        template='http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs=' #For 2019

        year=year
        url=template+month+'&aa_ifrs='+year #Create a handle, page, to handle the contents of the website
    elif site=='mwcl'or site=='MWCL' or site=='MarketWatchcl' or site =='Market Watch Chile':
        url='https://www.marketwatch.com/tools/markets/stocks/country/chile'
    else:
        return url
    return url





#Create pandas DataFrame from list in an html webpage
def scrap_lists(url):
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    try:
        page = requests.get(url, headers=agent)
    except:        
        print("Error:"+url+"\n  Trying By chunks")
        page = ld.get_url_bychunks(url)
    doc = lh.fromstring(page.content)#Parse data that are stored between <tr>..</tr> of HTML
    tr_elements = doc.xpath('//tr')
    #Check the length of the first 12 rows
    #print([len(T) for T in tr_elements[:12]])
    tr_elements = doc.xpath('//tr')#Create empty list
    col=[]
    i=0#For each row, store each first element (header) and an empty list
    for t in tr_elements[0]:
        i+=1
        name=t.text_content()
        #print ('%d:"%s"'%(i,name))
        col.append((name,[]))
    #Since out first row is the header, data is stored on the second row onwards
    for j in range(1,len(tr_elements)):
        #T is our j'th row
        T=tr_elements[j]
        
        #If row is not of the size of the headers, the //tr data is not from our table 
        if len(T)!=len(tr_elements[1]):
            break
        
        #i is the index of our column
        i=0
        
        #Iterate through each element of the row
        for t in T.iterchildren():
            data=t.text_content() 
            #Check if row is empty
            if i>0:
            #Convert any numerical value to integers
                try:
                    data=int(data)
                except:
                    pass
            #Append the data to the empty list of the i'th column
            col[i][1].append(data)
            #Increment i for the next column
            i+=1
    #print([len(C) for (title,C) in col])
    Dict={title:column for (title,column) in col}
    df=pd.DataFrame(Dict)
    return df
#########################################################
#########################################################
######################################################### 









# 
#Scrap fillings obtains the fillings from a provided list in zip format (XBRL inside)
#
def scrap_fillings(urls,filenames,update=0):
        #Download fillings according to obtained list of URLs
        for i in range (0,len(urls)):
            if filenames[i] == '0':
                continue
            else:
                url=urls[i]
                agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
                try: 
                    myfile = requests.get(url, headers=agent)
                except: #ChunkedEncodingError or  IncompleteRead        
                    print("Error:"+url+"\n  Trying By chunks")
                    myfile = ld.get_url_bychunks(url)
                wd=os.getcwd()
                #folder=Path(wd).parent
                #print(folder)
                datafold='/Data/Chile/'
                if update!=0 or not os.path.exists(wd+datafold+filenames[i]):
                    open(wd+datafold+filenames[i], 'wb').write(myfile.content)
                    temp=wd+datafold+filenames[i]
                    #download_wait(temp,wd+datafold)
                    if (temp!=temp.replace('.zip', '')):
                        temp=temp.replace('.zip', '')
                        if not os.path.exists(temp):
                            os.mkdir(temp)
                        print('Downloading ' + temp + '...\n')
                        downloaded = 1
                        time.sleep(1)
                        while downloaded!=0:
                            try:
                                with ZipFile(wd+datafold+filenames[i], 'r') as zipObj:
                            # Extract all the contents of zip file in different directory
                                    zipObj.extractall(temp)
                                    downloaded=0
                            except BadZipfile:
                                downloaded += 1
                                print('Still downloading '+' (' +str(downloaded*5)+' seconds )' + temp + ' \n')
                                time.sleep(5)
                            if downloaded>12:
                                print('error downloading ' + temp + 'Please download Manually before executing "allcompanies"')
                                break
                else:
                    print("Already downloaded "+ wd+datafold+filenames[i] + ' and Update not set')
#########################################################
#########################################################
#########################################################






#
#Function read_data  rescues previously saved data from csv
#Data is saved as a pandas DataFrame and returned
#

def read_data(filename,datafold='/',wd=0):
    if wd==0:
        wd=os.getcwd()
    else:
        wd=''
    if os.path.exists(wd+datafold+filename):
        df=pd.read_csv(wd+datafold+filename, header=0)
        df = df.astype(str)
    else:
        raise SystemExit('File not found in '+ wd+datafold+filename) 
    
    return df






#scrap_rutlists obtains lsit of profiles of companies who have filled recently
def scrap_rutlist(month,year):
    url=url_generator('cmf',month,year)
    df=scrap_lists(url)
    df = df.drop("Fecha Primer envío", axis=1)
    df = df.drop("Fecha último envío", axis=1)
    df = df.drop("Tipo Balance", axis=1)
    print(df.head())
    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='Ticker_data/registered_stocks_'+month+'-'+year+'.csv'
    datafold=wd+'/Data/Chile/'
    df.to_csv(datafold+file_name, index = None, header=True)




#scrap_MW obtains list of companies in the Santiago market
def scrap_mw():
    url=url_generator('mwcl','0','0')
    df=scrap_lists(url)
    df = df.drop("Exchange", axis=1)
    print(df.head())
    tickers=[]
    for i in range (0,df.shape[0]): #Here we extract tickers from the name list 
        tickers.append(df.loc[i, 'Name'])
        pos1=tickers[i].find('(')
        pos2=tickers[i].find(')')  # same as len just little easier to read
        tickers[i]=tickers[i][pos1+1:pos2]   
        pos1=pos2-pos1+1  #Saving variables, pos1 is now how many chars we are erasing at the end of the name (contained ticker)
        df.loc[i, 'Name']=(df.loc[i, 'Name'])[:-pos1]
    df['Ticker']=tickers

    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='registered_stocks.csv'
    datafold=wd+'/Data/Chile/'
    df.to_csv(datafold+file_name, index = None, header=True)

def scrap_offline():
    wd=os.getcwd()
    datafold='/Data/Chile/'
    file_name='list_of_tickers.pdf'
    filepdf = open(wd+datafold+file_name, 'rb')
    # pdf reader object
    filling = PyPDF2.PdfFileReader(filepdf)
    # # number of pages in pdf
    #print(filling)
    # # a page object
    table=[]
    #print(page.extractText())
    for i in range (0,filling.numPages):
        page = filling.getPage(i)
        temp=page.extractText()
        #table = tabula.read_pdf(wd+datafold+file_name,pages=i)
        table.append(temp.split('\n'))
    #print(table)
    filepdf.close()
    tickers=['Ticker']
    razon=['Name']
    count=0
    for pages in table:
        #print(pages)
        start=pages.index(str(1+count))
        for j in range((len(pages)-start)//3):
            tickers.append(pages[start+1+3*j])
            razon.append(pages[start+2+3*j])
            count=int(pages[start+3*j])
    final_list=[tickers,razon]
    temp_final_list=[]
    for j in range(0,len(tickers)):
        dim=[]
        for i in range(0,len(final_list)):
            dim.append(unidecode.unidecode(final_list[i][j]))
        temp_final_list.append(dim)
    final_list=temp_final_list
    column_names = final_list.pop(0)
    #print(column_names)
    df = pd.DataFrame(final_list, columns=column_names)
    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='registered_stocks.csv'
    datafold=wd+'/Data/Chile/'
    df.to_csv(datafold+file_name, index = None, header=True)




#
#Function Get_Ruts obtains the rut from a provided list
#Formatting is done to insert into url for scraping fillings
#

def get_ruts(df):
    #file_name='registered_stocks_'+month+'-'+year+'.csv'
    #datafold='/Data/Chile/'
    #df = df.astype(str)
    #We take out the verifier code of each rut since it is not used
    if not isinstance(df, int):
        for i in range (0,df.shape[0]):
            df.loc[i, 'Rut']=(df.loc[i, 'Rut'])[:-2]
    else:
        raise SystemExit('\nPlease Update not setting "Scrap" argument in "upandgetem" function ')    
    return df

#########################################################
#########################################################
#########################################################
#
# Tick2Rut get ruts from tickers lsit given, debugging functions are commented for including to final DataFrame
#

def Tick2Rut(ruts,tickers):
    rut_order=[]
    file_order=[]
    #test_order=[]
    #test_order2=[]

    ## Ranges for ¨For¨ Loops
    range1=tickers.shape[0]  
    range2=ruts.shape[0]


    for i in range (0,range1): #Here we pair right ticker with its rut
        rut_order.append('0')
        file_order.append('0')
        #test_order.append('0')
        #test_order2.append(0)
        curr_simil=0
        #print(ruts.head())
        #print(tickers.head())
        for j in range (0,range2):
            a=tickers.loc[i, 'Name']
            b=ruts.loc[j, 'Razón social']

            #Fix Formatting

            b=b.upper()
            a=a.upper()
            b=re.sub('Ñ','N',b)
            a=re.sub('Ñ','N',a)
            b=re.sub('Ñ','N',b)
            ### Fix Common errors
           
            #b=re.sub('S.A.','',b)
            #b=re.sub('ADMINISTRADORA GENERAL DE FONDOS','',b)
            #a=re.sub('S.A.','',a)
            #a=re.sub('ADMINISTRADORA GENERAL DE FONDOS','',a)
            #a=re.sub('A.F.P.','ADMINISTRADORA  DE FONDOS DE PENSIONES',a)
            #a=re.sub('FONDO DE INVERSION','',a)

            ##Some abbreviations cause problems too, company specific hardcoding below

            #a=re.sub('ARBITRAGE','',a)

            simil=SequenceMatcher(None, a, b).ratio() #We compare names
            #Most similar name stays
            if simil > curr_simil:
                rut_order[i]=ruts.loc[j, 'Rut']
                file_order[i]=ruts.loc[j, 'Tipo Envio']
                curr_simil=simil
                #test_order[i]=b
                #test_order2[i]=simil
            if j==(range2-1) and curr_simil<0.90:
                    rut_order[i]= 'ERROR HIGH: BANK OR NOT FOUND'   
            #Not checking errors anymore since doesn't have any
            #print(a)
            #print(b)
            #print('Similitud=')
            #print(curr_simil)
            #print('\n')
    #tickers['Check']=test_order  
    #tickers['SIMIL']=test_order2      
    tickers['Rut']=rut_order
    tickers['File']=file_order
    return tickers


def tick2code(datafold,names): # We search for the tickers corresponding to the banks and obtain the IFI Code of each
    #If the ticker is not found we leave the institution name as the ticker for completeness purposes
    df=read_data('registered_stocks.csv',datafold,wd=1)
    tickers=[df['Ticker'].tolist(),df['Name'].tolist()]
    for i in range(len(names[0])):
        for j in range(len(tickers[0])): 
            #print('name')       
            #print(names[0][i])
            if SequenceMatcher(None, tickers[1][j].lower(), names[0][i].lower()).ratio()>0.8:#They are usually the same, but we put 0.8 just in case     
                names[0][i]=tickers[0][j]
                #print('found ticker') 
                #print(tickers[0][j])
                break
    return names


            


def bruteforce_bank_scrap(month,year, month2='03', year2='2020',update=0):
    #original link for march 2020 https://www.sbif.cl/sbifweb3/internet/archivos/Info_Fin_7877_19022.zip
    #From month/year to month2/year2
    if int(str(datetime.now())[0:4])<int(year2) or (int(str(datetime.now())[0:4])==int(year2) and int(str(datetime.now())[5:7])<=(int(month2)-2)):
        print('The date solicited is too recent, please put another date or download the last month manually')
        return
    month=int(month)
    year=int(year)
    month2=int(month2)
    year2=int(year2)
    previous_months=(2020-year)*12-((month))+4#We calculate how many months unitl march 2020 and we include march 2020
    next_months=(year2-2020)*12+((month2))-3 #We calculate how many months from march 2020
    print(previous_months)
    wd=os.getcwd()
    datafold='/Data/Chile/Banks/'
    if not os.path.exists(wd+datafold):
        os.mkdir(wd+datafold)
    i=0
    number_of_files=0
    name_month=3
    name_year=2020
    while number_of_files<previous_months and i<MAXMONTHS*previous_months:
        url='https://www.sbif.cl/sbifweb3/internet/archivos/Info_Fin_7877_'+str(19022-i)+'.zip'
        i+=1
        if name_month<10:
            stname_month='0'+str(name_month)
        else:
            stname_month=str(name_month)
        filename='Banks_'+stname_month+'-'+str(name_year)
        name_month=int(name_month)
        name_year=int(name_year)
        if update==0 or not os.path.exists(wd+datafold+filename):
            agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
            myfile = requests.get(url, headers=agent)            
            page=lh.fromstring(myfile.content)
            #We obtain every <a and take the URLS to a list
            test = page.xpath('//h1[contains(@class,"titulo")]')
            #print(test)
            if not test:
                open(wd+datafold+filename+'.zip', 'wb').write(myfile.content)
                print('Downloading ' + filename + '...\n')
                downloaded = 1
                time.sleep(1)
                while downloaded!=0:
                    try:
                        print(wd+datafold+filename+'.zip')
                        with ZipFile(wd+datafold+filename+'.zip', 'r') as zipObj:
                    # Extract all the contents of zip file in different directory
                            if not os.path.exists(wd+datafold+filename+'/'):                                
                                os.mkdir(wd+datafold+filename+'/')
                            zipObj.extractall(wd+datafold+filename+'/')
                            if glob.glob(wd+datafold+filename+'/'+str(name_year)+stname_month+'*'):                                    
                                number_of_files+=1
                                if name_month>1:
                                    name_month-=1
                                else:
                                    name_month=12
                                    name_year-=1  
                                strip_spaces(wd+datafold+filename+'/')
                            else:
                                try:
                                    shutil.rmtree(wd+datafold+filename+'/')
                                except OSError as e:
                                    print ("Error: %s - %s." % (e.filename, e.strerror))
                            downloaded=0
                    except BadZipfile:
                        downloaded += 1
                        print('Still downloading '+' (' +str(downloaded)+' seconds )' + filename + '.zip' +' \n')
                        time.sleep(5)
                    if downloaded>12:
                        print('error downloading ' + filename +'.zip' + 'Please download Manually before executing "allcompanies"')
                        break
   
        else:
            print("Already downloaded "+ wd+datafold+filename +  '/' + filename+ '.zip' + ' and Update not set')
            if name_month>1:
                name_month-=1
            else:
                name_month=12
                name_year-=1  
    ######################################################
    ### Here the same but for months afrter the 03/2020 
    ######################################################
    
    number_of_files=0
    name_month=4
    name_year=2020
    i=1
    while number_of_files<next_months and i<MAXMONTHS*next_months:
        url='https://www.sbif.cl/sbifweb3/internet/archivos/Info_Fin_7877_'+str(19022+i)+'.zip'
        i+=1
        if name_month<10:
            stname_month='0'+str(name_month)
        else:
            stname_month=str(name_month)        
        filename='Banks_'+stname_month+'-'+str(name_year)
        name_month=int(name_month)
        name_year=int(name_year)
        if update==0 or not os.path.exists(wd+datafold+filename+'.zip'):
            agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
            myfile = requests.get(url, headers=agent)            
            page=lh.fromstring(myfile.content)
            #We obtain every <a and take the URLS to a list
            test = page.xpath('//h1[contains(@class,"titulo")]')
            #print(test)
            if not test:
                open(wd+datafold+filename+'.zip', 'wb').write(myfile.content)
                print('Downloading ' + filename + '...\n')
                downloaded = 1
                time.sleep(1)
                while downloaded!=0:
                    try:
                        print(wd+datafold+filename+'.zip')
                        with ZipFile(wd+datafold+filename+'.zip', 'r') as zipObj:
                    # Extract all the contents of zip file in different directory
                            if not os.path.exists(wd+datafold+filename+'/'):                                
                                os.mkdir(wd+datafold+filename+'/')
                            zipObj.extractall(wd+datafold+filename+'/')
                            if glob.glob(wd+datafold+filename+'/'+str(name_year)+stname_month+'*'):   
                                number_of_files+=1
                                if name_month<12:
                                    name_month+=1
                                else:
                                    name_month=1
                                    name_year+=1                                 
                                strip_spaces(wd+datafold+filename+'/')
                            else:
                                try:
                                    shutil.rmtree(wd+datafold+filename+'/')
                                except OSError as e:
                                    print ("Error: %s - %s." % (e.filename, e.strerror))
                            downloaded=0
                    except BadZipfile:
                        downloaded += 1
                        print('Still downloading '+' (' +str(downloaded)+' seconds )' + filename + '.zip' +' \n')
                        time.sleep(5)
                    if downloaded>12:
                        print('error downloading ' + filename +'.zip' + 'Please download Manually before executing "allcompanies"')
                        break
        else:
            print("Already downloaded "+ wd+datafold+filename + '.zip' + ' and Update not set')  
            if name_month<12:
                name_month+=1
            else:
                name_month=1
                name_year+=1 

    print('Finished Download of bank s data')


def read_bank_codes(datafold):
    #First we read the codes for the names of the banks
    ####################################################
    ####################################################
    try:
        df=pd.read_csv(datafold+'Instrucciones/CODIFIS.TXT', skiprows=3 ,delimiter='\t',index_col=False ,encoding ='latin1')       
    except pd.errors.ParserError:
        wd=os.getcwd()        
        df=pd.read_csv(wd+'/Data/Chile/Banks/Banks_03-2020/202003-290420/Instrucciones/CODIFIS.TXT', skiprows=3 ,delimiter='\t',index_col=False ,encoding ='latin1')
    except FileNotFoundError:
        df=pd.read_csv(datafold+'Instrucciones/Instrucciones/CODIFIS.TXT', skiprows=3 ,delimiter='\t',index_col=False ,encoding ='latin1')
    codes=df['COD. IFI'].tolist()
    names=df['RAZON SOCIAL'].tolist()
    names=[(name.replace('(','')).replace(')','') for name in names if name==name]
    for i in range(len(names)):
        numbinnames=[s for s in names[i].split() if s.isdigit()]
        for numb in numbinnames:
            names[i]=names[i].replace(numb,'')
    codes=codes[:len(names)]
    codes.pop(-1)
    names.pop(-1)
    names=[names,codes]
    ####
    #Then we read the codes of the bank actives in MB1 file (Consolidated Monthly Balance file)
    ####################################################
    ####################################################
    df=pd.read_csv(datafold+'Instrucciones/Modelo-MB1.TXT', skiprows=1 ,delimiter='\t',header=0,names=['code', 'parameter'] ,usecols=['code', 'parameter'],index_col=False,encoding ='latin1')       
    #print(df['code'])
    #print(df['parameter'])
    codes=df['code'].tolist()
    parameter=df['parameter'].tolist()
    n=len(parameter)
    for j in range(n):
        i=n-1-j
        if parameter[i]==parameter[i] and codes[i]==codes[i]:
            #print(parameter[i])
            parameter[i]=parameter[i].strip()
            codes[i]=str(codes[i]).strip()
        else:
            #print(parameter[i])
            parameter.pop(i)
            codes.pop(i)
    parameter=[unidecode.unidecode(((para.replace('(','')).replace(')','')).replace('*','')) for para in parameter]
    codes=[code.strip() for code in codes]
    mb1=[parameter,codes]
        ####
    #Then we read the codes of the bank actives in MR1 file (Consolidated Monthly Results file)
    ####################################################
    ####################################################
    df=pd.read_csv(datafold+'Instrucciones/Modelo-MR1.TXT', skiprows=1 ,delimiter='\t',header=0,names=['code', 'parameter'] ,usecols=['code', 'parameter'],index_col=False,encoding ='latin1')       
    #print(df['code'])
    #print(df['parameter'])
    codes=df['code'].tolist()
    parameter=df['parameter'].tolist()
    n=len(parameter)
    for j in range(n):
        i=n-1-j
        if parameter[i]==parameter[i] and codes[i]==codes[i]:
            #print(parameter[i])
            parameter[i]=parameter[i].strip()
            codes[i]=str(codes[i]).strip()
        else:
            #print(parameter[i])
            parameter.pop(i)
            codes.pop(i)
    parameter=[unidecode.unidecode(((para.replace('(','')).replace(')','')).replace('*','')) for para in parameter]
    codes=[code.strip() for code in codes]
    mr1=[parameter,codes]
    df=pd.read_csv(datafold+'Instrucciones/Modelo-MC1.TXT', skiprows=1 ,delimiter='\t',header=0,names=['code', 'parameter'] ,usecols=['code', 'parameter'],index_col=False,encoding ='latin1')       
    #print(df['code'])
    #print(df['parameter'])
    codes=df['code'].tolist()
    parameter=df['parameter'].tolist()
    n=len(parameter)
    for j in range(n):
        i=n-1-j
        if parameter[i]==parameter[i] and codes[i]==codes[i]:
            #print(parameter[i])
            parameter[i]=parameter[i].strip()
            codes[i]=str(codes[i]).strip()
        else:
            #print(parameter[i])
            parameter.pop(i)
            codes.pop(i)
    parameter=[unidecode.unidecode(((para.replace('(','')).replace(')','')).replace('*','')) for para in parameter]
    codes=[code.strip() for code in codes]
    mc1=[parameter,codes]
    return names,mb1,mr1,mc1

    #return names

def get_bank_tickers(datafold):
    names,mb1,mr1,mc1=read_bank_codes(datafold)
    names=tick2code(datafold,names)
    return names,mb1,mr1,mc1


def strip_spaces(datafold):
    #print(datafold + "/**/*.txt")
    #print(glob.glob(datafold + "/**/*.txt", recursive=True))
    print('Formating files ...')
    for files in glob.glob(datafold + "/**/*.txt", recursive=True):
        #print(files)
        clean_lines = []
        with open(files, "r", encoding='latin1') as f:
            lines = f.readlines()
            clean_lines = [(l.strip()).strip('\t') for l in lines if l.strip()]
        with open(files, "w", encoding='latin1') as f:
            f.writelines('\n'.join(clean_lines))



#def 
#get_bank_tickers(datafold,filename)

#scrap_mw()
#bruteforce_bank_scrap('03','2012', '03', '2020')

#scrap_offline()



#print(df.head())
#print(df.shape[1])

#df=scrape_list(month,year)
#scrap_fillings(df,n,month, year)
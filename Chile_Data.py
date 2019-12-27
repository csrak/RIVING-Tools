import requests
import lxml.html as lh
import pandas as pd
import os
from pathlib import Path
#from bs4 import BeautifulSoup


###
#####
###### 
#    Functions for scraping data, general functions and some hard-code for specific scraping is found here
#       Specifically for importing data from chilean stocks, into csv file, mostly into ~/data/chile/ folder

month='03'
year='2019'


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
    file_name='registered_stocks_'+month+'-'+year+'.xls'
    datafold='\\Data\\Chile\\'
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
def scrape_lists(url):
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    page = requests.get(url, headers=agent)
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
def scrap_fillings(companies_list,n,month, year):
        #Download 
       # http://www.cmfchile.cl/institucional/inc/inf_financiera/ifrs/safec_ifrs_verarchivo.php?auth=&send=&
       # rut=77750920&mm=03&aa=2019&archivo=77750920_201903_I.zip&desc_archivo=Estados%20financieros%20(XBRL)&tipo_archivo=XBRL
    template = 'http://www.cmfchile.cl/institucional/inc/inf_financiera/ifrs/safec_ifrs_verarchivo.php?auth=&send=&'
    for  i in n:
        rut=companies_list[i,2]  ##2 is the column of ruts

        url=template+'rut='+rut+'&mm='+month+'&aa='+year+'&archivo='+rut+'_'+year+month+'_I.zip&desc_archivo=Estados%20financieros%20(XBRL)&tipo_archivo=XBRL'

        agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
        myfile = requests.get(url, headers=agent)

        wd=os.getcwd()
        #folder=Path(wd).parent
        #print(folder)
        file_name=rut+'_'+month+year+'.zip'
        datafold='\\Data\\Chile\\'
        open(wd+datafold+file_name, 'wb').write(myfile.content)
#########################################################
#########################################################
#########################################################






#
#Function read_data  rescues previously saved data from csv
#Data is saved as a pandas DataFrame and returned
#

def read_data(filename,datafold):
    wd=os.getcwd()
    if os.path.exists(wd+datafold+filename):
        df=pd.read_csv(wd+datafold+filename, header=0)
    else:
        print('File not found in '+ wd+datafold+filename)
        return 0
    
    return df






#scrap_rutlists obtains lsit of profiles of companies who have filled recently
def scrap_rutlist(month,year):
    url=url_generator('cmf',month,year)
    df=scrape_lists(url)
    df = df.drop("Fecha Primer envío", axis=1)
    df = df.drop("Fecha último envío", axis=1)
    df = df.drop("Tipo Balance", axis=1)
    print(df.head())
    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='registered_stocks_'+month+'-'+year+'.csv'
    datafold=wd+'\\Data\\Chile\\'
    export_csv = df.to_csv(datafold+file_name, index = None, header=True)




#scrap_MW obtains list of companies in the Santiago market
def scrap_mw():
    url=url_generator('mwcl','0','0')
    df=scrape_lists(url)
    df = df.drop("Exchange", axis=1)
    print(df.head())
    tickers=[]
    for i in range (0,df.shape[0]): #Here we extract tickers from the name list 
        tickers.append(df.loc[i, 'Name'])
        pos1=tickers[i].find('(')
        pos2=tickers[i].find(')')  # same as len just little easier to read
        tickers[i]=tickers[i][pos1+1:pos2]   
        pos1=pos2-pos1+1  #Saving variables, pos1 is now how many chars we are erasing
        df.loc[i, 'Name']=(df.loc[i, 'Name'])[:-pos1]
    df['Ticker']=tickers

    wd=os.getcwd()
    #folder=Path(wd).parent
    #print(folder)
    file_name='registered_stocks_mw.csv'
    datafold=wd+'\\Data\\Chile\\'
    export_csv = df.to_csv(datafold+file_name, index = None, header=True)



#
#Function Get_Ruts obtains the rut from a provided list
#Formatting is done to insert into url for scraping fillings
#

def get_ruts(month, year,file_name, datafold):
    #file_name='registered_stocks_'+month+'-'+year+'.csv'
    #datafold='\\Data\\Chile\\'
    df=read_data(file_name,datafold)
    df = df.astype(str)

    #We take out the verifier code of each rut since it is not used
    for i in range (0,df.shape[0]):
        df.loc[i, 'Rut']=(df.loc[i, 'Rut'])[:-2]      
    return df.loc[:, 'Rut']

#########################################################
#########################################################
#########################################################

scrap_mw()





#
#file_name='registered_stocks_'+month+'-'+year+'.csv'
#datafold='\\Data\\Chile\\'
#ruts=get_ruts(month,year,file_name,datafold)
#print(ruts)


#print(df.head())
#print(df.shape[1])

#df=scrape_list(month,year)
#scrap_fillings(df,n,month, year)
import requests
import pandas as pd
import os
import glob
import numpy as np
from lxml import etree
import lxml.html as lh
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def nasdaq_list(wd, update = 1):
    ticker= []
    df = []
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
    myfile = requests.get(url, headers=agent)
    print ("Parsing %s"%(url))
    if update == 1:
        open(wd +'/Data/US/nasdaqlisted.txt', 'wb').write(myfile.content)
    with open(wd +'/Data/US/nasdaqlisted.txt') as list_n:
        df = pd.read_csv(list_n, delimiter = "|" )
        df.drop(labels=df.index[-1], inplace=True)
        ticker = df['Symbol'].tolist()
        ticker = [i.lower() for i in ticker]
    return ticker

def sec_list(wd, update = 1):
    if update == 1:
        agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
        url = 'https://www.sec.gov/include/ticker.txt'
        myfile = requests.get(url, headers=agent)
        print ("Parsing %s"%(url))    
        open(wd +'/Data/US/ticker.txt', 'wb').write(myfile.content)
    with open(wd +'/Data/US/ticker.txt') as list_cik:
        df = pd.read_csv(list_cik, delimiter = "\t",names=['ticker', 'cik'], dtype={'name':str,'cik':str})
    #print(df)    
    return df

def scrap_4f(cik, number = 1,skip = 0, folder = 0):
    #We give a single cik of the company to retrieve 
    #number is the amount of fillings we want (selecting dates will be added later)
    #skip is how many of the initial ones we don't want, we will retrieve "number" amount of the ones after these
    #folder option lets you download to an exsiting folder instead of just retrieving link
    number = number + skip
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=&dateb=&owner=only&count=100&search_text='
    page = requests.get(url, headers=agent) 
    page=lh.fromstring(page.content)
    #We obtain every <a and take the URLS to a list
    link = page.xpath('//a')
    out=[np.nan]*number  
    #We search the search page for 4 fillings (first 100 results due to url)
    c = 0
    for links in link:
        if c >= number:
            break
        if 'href' in links.attrib and '/Archives/edgar/data/' in links.attrib['href']:
            separators = [pos for pos, char in enumerate(links.attrib['href']) if char == '/']
            out[c]='https://www.sec.gov'+(links.attrib['href'])[0:separators[-1]]
            c+=1       
    if folder != 0 and os.path.exists(folder):
        for i in range(number):                 
            myfile = requests.get(out[i+skip] +'/form4.xml', headers=agent)
            if 'The specified key does not exist' in str(myfile.content):
                out[i+skip] = out[i+skip] +'/doc4.xml'  #different name, changed in list
                myfile = requests.get(out[i+skip], headers=agent)
            else:
                out[i+skip] = out[i+skip] +'/form4.xml'            
            separators = [pos for pos, char in enumerate(out[i+skip]) if char == '/']
            name=out[i+skip][separators[-2]:separators[-1]]            
            open(folder +name+'.xml', 'wb').write(myfile.content)
    return out[skip:]


def get_4f(wd, ticker_list, number = 1):
    if not os.path.exists(wd+'/Data/US/4F/'):
        os.mkdir(wd+'/Data/US/4F/')
    tck_list = sec_list(wd, update = 0)
    #print(tck_list)
    for ticker in ticker_list:
        try:
            cik=tck_list.loc[pd.Index(tck_list['ticker']).get_loc(str(ticker).lower()),'cik']
            #print(cik)
            #print("ticker and "+str(cik))            
            if not os.path.exists(wd+'/Data/US/4F/'+ticker+'/'):
                os.mkdir(wd+'/Data/US/4F/'+ticker+'/')
            scrap_4f(cik, number = number, folder = wd+'/Data/US/4F/'+ticker+'/')          
        except KeyError:
            print("Ticker "+str(ticker)+" not found in CIK list") 


def read_4f(folder, param_list):    
    results=[]
    os.chdir(folder)
    for file in glob.glob("*.xml"):
        fillings = etree.parse(file)      
        for param in param_list:            
            params = fillings.xpath('//'+param) 
            for fparam in params:
                results.append(fparam.text)
        #fillings = parser.close()
        stock = fillings.findall('//'+'nonDerivativeHolding')
        for shares in stock:
            try:
                share=shares.find('postTransactionAmounts')
                try:
                    shar=share.find('sharesOwnedFollowingTransaction')
                    try:
                        sha=shar.find('value')            
                    except:
                        continue
                except:
                    continue
            except:
                continue
            print(sha.text)
        stock = fillings.findall('//nonDerivativeTransaction')
        for shares in stock:
            try:
                dates=shares.find('transactionDate')
                share = shares.find('transactionAmounts')
                try:
                    date=dates.find('value')
                    sharn=share.find('transactionShares')
                    nshares=sharn.find('value')
                    print(date.text + '    ' + nshares.text)
                except:
                    print("Transaction info not found for file"+file)
                    continue
            except:
                print("Transaction not found for file"+file)
                continue
    print(results)

#list_of_param
#wd=os.getcwd()
basic_param = ['issuerCik','issuerTradingSymbol','rptOwnerCik','isDirector','isOfficer','isTenPercentOwner','isOther','officerTitle']

trans_param = ['nonDerivativeHolding']

folder = "C:/Users/csrak/Desktop/python/RIVING-Tools/Data/US/4F/AMZN/"
#ticker_list = ['AMZN','KO',234]
#get_4f(wd, ticker_list, number = 5)
read_4f(folder, basic_param)
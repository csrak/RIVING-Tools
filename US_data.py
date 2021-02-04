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


def read_4f(folder, param_list, param_def_list = []):
    #Input folder and optional parameters derired
    #Non-optional parameters such as date, nr of shares, share value, two codes of transaction and holdings are always obtained
    #As an option it is possible to add a default value to the param_list in case one of the values is not found
    #By default it is just filled with np.nan 
    if param_def_list == []:
        param_def_list = [np.nan for par in param_list]
    elif len(param_def_list) != len(param_list):
        raise Exception("Please use a full default parameter list or don't use it at all") 
    os.chdir(folder)
    list_df = []
    for file in glob.glob("*.xml"): 
        fillings = etree.parse(file)      
        results=[]
        c = 0
        for param in param_list:   
            params = fillings.xpath('//'+param) 
            if len(params) == 1:                  
                results.append(params[0].text)
            elif len(params) == 0:
                results.append(param_def_list[c])
            else:
                raise Exception("Unexpected multiple instances of ",param, 'in file ', file) #Optional parameters should only have one instance per file, which is why this is checked
            c = c+1         
        #fillings = parser.close()
        holdings = fillings.findall('//'+'nonDerivativeHolding')
        total_hold = 0.0
        for holding in holdings: #Holding sum
            try:
                holdin=holding.find('postTransactionAmounts')
                try:
                    holdi=holdin.find('sharesOwnedFollowingTransaction')
                    try:
                        hold=holdi.find('value')            
                    except:
                        print("Holding info not found for file"+file)
                        continue
                except:                   
                    print("Holding info not found for file"+file)
                    continue
            except:
                print("Holding info not found for file"+file)
                continue
            total_hold = float(hold.text)+total_hold #We get the total holdings in not transactioned accounts
        results = [total_hold]+results #We will create a list of list to form each row of the df, elements until now are common so will be
                                        # appended to all elements from this file

        stock = fillings.findall('//nonDerivativeTransaction')        
        #derivatives = fillings.find('//derivativeTable')
        derivatives = fillings.findall('//derivativeTransaction')

        for shares in stock: #Non-derivative transaction parser
            data = [np.nan,np.nan,np.nan,np.nan,np.nan]
            try:
                dates=shares.find('transactionDate')
                share = shares.find('transactionAmounts')
                coding = shares.find('transactionCoding')
                try:
                    date=dates.find('value')
                    sharn=share.find('transactionShares')
                    nshares=sharn.find('value')
                    sharevalue = share.find('transactionPricePerShare')
                    shareval = sharevalue.find('value')                    
                    code = coding.find('transactionCode')
                    acquiredordisposed=share.find('transactionAcquiredDisposedCode')
                    acquiredordisposed = acquiredordisposed.find('value')
                    data = [date.text,acquiredordisposed.text,float(nshares.text),float(shareval.text),code.text]
                except:
                    print("Transaction info not found for file"+file)
                    continue
            except:
                print("Transaction not found for file"+file)
                continue
            data = data+results
            list_df.append(data)
        for shares in derivatives:
             #derivative transaction parser
            try:
                dates=shares.find('transactionDate')
                share = shares.find('transactionAmounts')
                coding = shares.find('transactionCoding')
                try:
                    date=dates.find('value')
                    sharn=share.find('transactionShares')
                    nshares=sharn.find('value')
                    sharevalue = share.find('transactionPricePerShare')
                    shareval = sharevalue.find('value')                    
                    code = coding.find('transactionCode')
                    print(date.text + '    ' + nshares.text + '   ' + shareval.text+ '   ' + code.text)
                except:
                    print("Transaction info not found for file"+file)
                    continue
            except:
                print("Transaction not found for file"+file)
                continue
    table = pd.DataFrame(list_df,columns=(['date','acquiredordisposed','shares','price','code','staticholdings']+param_list))
    print(table)
    return table

#list_of_param
#wd=os.getcwd()
basic_param = ['issuerCik','issuerTradingSymbol','rptOwnerCik','isDirector','isOfficer','isTenPercentOwner','isOther','officerTitle']
def_basic_param = [np.nan,'',np.nan,0,0,0,0,'']

trans_param = ['nonDerivativeHolding']

folder = "C:/Users/csrak/Desktop/python/RIVING-Tools/Data/US/4F/AMZN/"
#ticker_list = ['AMZN','KO',234]
#get_4f(wd, ticker_list, number = 5)
read_4f(folder, basic_param,def_basic_param)
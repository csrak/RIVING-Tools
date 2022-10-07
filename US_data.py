#from _typeshed import NoneType
import requests
import pandas as pd
import os
import glob
import numpy as np
from lxml import etree as et
import lxml.html as lh
import urllib3
import time
import datetime
import re
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import random

class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def writelines(self, datas):
       self.stream.writelines(datas)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

import sys
sys.stdout = Unbuffered(sys.stdout)


user_agent_list = ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15','Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36','Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36','Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36']

header_done= {"User-Agent": "PUC Cris cuenta.de.r2d2@gmail.com", "Accept-Encoding": "gzip, deflate", "Host": "www.sec.gov" }
def nasdaq_list(wd, update = 1):
    ticker= []
    df = []
    #agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
    myfile = requests.get(url, headers=header_done)
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
        #agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
        url = 'https://www.sec.gov/include/ticker.txt'
        myfile = requests.get(url, headers=header_done)
        print ("Parsing %s"%(url))    
        open(wd +'/Data/US/ticker.txt', 'wb').write(myfile.content)
    with open(wd +'/Data/US/ticker.txt') as list_cik:
        df = pd.read_csv(list_cik, delimiter = "\t",names=['ticker', 'cik'], dtype={'name':str,'cik':str})
    #print(df)    
    return df

def scrap_4f(cik, number = 0,skip = 0, folder = 0, method = 'dates', dfrom = '2010-01-01', duntil = ''):
    #We give a single cik of the company to retrieve 
    #number is the amount of fillings we want (selecting dates will be added later)
    #"skip" is how many of the initial ones we don't want, we will retrieve a "number" amount of the ones after the first "skip" number
    #folder option lets you download to an exsiting folder instead of just retrieving link
    if dfrom != '':
        dfrom = datetime.datetime.strptime(dfrom, '%Y-%m-%d')
    if duntil != '':
        duntil = datetime.datetime.strptime(duntil, '%Y-%m-%d')
    if (method == 'number'):
        c = 0
        number = number + skip
        start = 0
        out=[np.nan]*number
        link=[]
        while c < number:
            #random.seed(datetime.datetime.now())
            #user_agent= random.choice(user_agent_list)
            #agent = {"User-Agent":user_agent}            
            url='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=&dateb=&owner=only&start='+str(start)+'&count='+str(start+100)+'&search_text='
            page = requests.get(url, headers=header_done)
            waitingtime = 1
            while ('Rate Threshold Exceeded' in page.text or 'Internet Security Policy' in page.text):
                #user_agent= random.choice(user_agent_list)
                waitingtime = waitingtime + 1
                time.sleep(waitingtime)
                print("Got detected as bot network, waiting to try again... \n")
                page = requests.get(url, headers=header_done)
            page=lh.fromstring(page.content)
            time.sleep(0.3) #To not get kicked out of EDGAR
            #We obtain every <a and take the URLS to a list
            link = page.xpath('//a')
            #We search the search page for 4 fillings (first 100 results due to url)
            for links in link:
                if c >= number:
                    break
                if 'href' in links.attrib and '/Archives/edgar/data/' in links.attrib['href']:
                    separators = [pos for pos, char in enumerate(links.attrib['href']) if char == '/']
                    out[c]='https://www.sec.gov'+(links.attrib['href'])[0:separators[-1]]
                    c+=1    
                #else:
                #     print("Link not found in : ", url)
            if folder != 0 and os.path.exists(folder):
                for i in range(start,c):  
                    if i<skip:
                        continue
                    page = requests.get(out[i], headers=header_done) 
                    page=lh.fromstring(page.content)
                    #We obtain every <a and take the URLS to a list
                    filess = page.xpath('//a')
                    file_link = out[i]
                    for files in filess:
                        if 'href' in files.attrib and ".xml" in files.attrib['href']:
                            file_link = 'https://www.sec.gov'+files.attrib['href']
                            break
                    myfile = requests.get(file_link, headers=header_done)         
                    separators = [pos for pos, char in enumerate(file_link) if char == '/']
                    name=file_link[separators[-2]:separators[-1]]            
                    open(folder +name+'.xml', 'wb').write(myfile.content)
            if len(link)<100:
                break
            else:
                start+=100
        return out[skip:]
    elif method == 'dates':#For now separate method until we know it is bugless
        out = []
        number = number + skip
        start = 0
        out=[np.nan]*number
        link=[]
        table_f = pd.DataFrame()
        while (start < number or number == 0):  
            #random.seed(datetime.datetime.now())
            #user_agent= random.choice(user_agent_list)
            #agent = {"User-Agent":user_agent}            
            url='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=&dateb=&owner=only&start='+str(start)+'&count='+str(start+100)+'&search_text='
            page = requests.get(url, headers=header_done)
            print(url) 
            waitingtime = 1
            while ('Rate Threshold Exceeded' in page.text or 'Internet Security Policy' in page.text):
                #user_agent= random.choice(user_agent_list)
                waitingtime = waitingtime + 1
                time.sleep(waitingtime)
                print("Got detected as bot network, waiting to try again... \n")
                page = requests.get(url, headers=header_done) 
            table_f_temp = pd.DataFrame()
            tables = pd.read_html(page.content)  
            #print(tables)
             #To not get kicked out of EDGAR
            time.sleep(0.3)          
            for tabl in tables:
                if 'Filing Date' in tabl:
                    table_f_temp = tabl
            table_f_temp_temp = table_f_temp
            table_f_temp.loc[:,'Filings'] = pd.to_numeric(table_f_temp['Filings'], errors='coerce')
            #We obtain every <a and take the URLS to a li st
            table_f_temp.loc[:,'Filing Date'] = pd.to_datetime(table_f_temp['Filing Date'],format="%Y-%m-%d") 
            table_f_temp = table_f_temp[table_f_temp['Filings']==4] #Pandas is stupid, if you do any type conversion right after changing through this method, then it throws a copy warning, which is why this is here and not before
            if len(table_f_temp.index)== 0:
                print(table_f_temp_temp)
                print("Nothing here in this last url")
                break
            start = start+100
            if start >100:
                table_f = pd.concat([table_f, table_f_temp])
            else:
                table_f = table_f_temp
            table_f_temp = table_f_temp[table_f_temp['Filing Date'] < dfrom]
            if len(table_f_temp.index) > 0: #check if we reached the last date we wanted to get out of the loop
                break
        table_f = table_f[table_f['Filing Date'] > dfrom] 
        if duntil != '':
            table_f = table_f[table_f['Filing Date'] < duntil] 
        listoffillings = table_f['Description'].tolist()        
        listoffillings = [re.search(r'\d\d\d\d\d\d\d\d\d\d-\d\d-\d\d\d\d\d\d',i).group() for i in listoffillings]#Look for code of filling from which we will create the link
        listofnames = [i.replace('-','') for i in listoffillings]
        listoffillings = ['https://www.sec.gov/Archives/edgar/data/'+cik+'/'+i.replace('-','') for i in listoffillings]
        if folder != 0 and os.path.exists(folder):
            counter = 0
            for i in listoffillings: 
                notdone = True 
                while notdone == True:
                    try:
                        #random.seed(datetime.datetime.now())
                        #user_agent= random.choice(user_agent_list)
                        #agent = {"User-Agent":user_agent}  
                        page = requests.get(i, headers=header_done) 
                        waitingtime = 1
                        while ('Rate Threshold Exceeded' in page.text or 'Internet Security Policy' in page.text):
                            waitingtime = waitingtime + 1
                            time.sleep(waitingtime)
                            print("Got detected as bot network, waiting to try again... \n")
                            #user_agent= random.choice(user_agent_list)
                            page = requests.get(i, headers=header_done)
                        page=lh.fromstring(page.content)
                        #We obtain every <a and take the URLS to a list
                        filess = page.xpath('//a')#files in the web directory
                        file_link = i
                        for files in filess:
                            if 'href' in files.attrib and ".xml" in files.attrib['href']:
                                file_link = 'https://www.sec.gov'+files.attrib['href']
                                print(file_link)
                                break
                        myfile = requests.get(file_link, headers=header_done)         
                        name=listofnames[counter]   
                        #(name)         
                        open(folder +name+'.xml', 'wb').write(myfile.content)
                        time.sleep(0.25) #To not get kicked out of EDGAR
                        counter = counter +1
                        notdone = False
                    except urllib3.exceptions.ProtocolError:
                        print("Connection error, or rejected from website, trying again")
                        notdone = True
                    except urllib3.exceptions.ProtocolError:
                        print("Connection error, or rejected from website, trying again")
                        time.sleep(2)
                        notdone = True
                    except KeyboardInterrupt:
                        exit()
                    except: 
                        print("Connection error, or rejected from website, trying again")
                        time.sleep(2)
                        notdone = True
                        

        return listoffillings


def get_4f(wd, ticker_list, number = 0, method = 'dates', dfrom = '2010-01-01', duntil = '2021-12-31'):
    if not os.path.exists(wd+'/Data/US/4F/'):
        os.mkdir(wd+'/Data/US/4F/')
    tck_list = sec_list(wd, update = 0)
    #print(tck_list)
    for ticker in ticker_list:
        print("Downloading 4F fillings for ", ticker)
        try:
            cik=tck_list.loc[pd.Index(tck_list['ticker']).get_loc(str(ticker).lower()),'cik']
            #print(cik)
            #print("ticker and "+str(cik))            
            if not os.path.exists(wd+'/Data/US/4F/'+ticker+'/'):
                os.mkdir(wd+'/Data/US/4F/'+ticker+'/')
            scrap_4f(cik, number = number, folder = wd+'/Data/US/4F/'+ticker+'/', method =  method , dfrom = dfrom, duntil = duntil)          
        except KeyError:
            print("Ticker "+str(ticker)+" not found in CIK list or there were problems with its data") 
    os.chdir(wd+'/Data/US/4F/')
    file1 = open("DatesOfExe.txt","w")
    file1.write(dfrom + " - " + datetime.datetime.today().strftime('%Y-%m-%d')
)

def read_4f(folder_i, param_list, param_def_list = [],company = "", to_file = False):
    #Input folder and optional parameters derired
    #Non-optional parameters such as date, nr of shares, share value, two codes of transaction and holdings are always obtained
    #As an option it is possible to add a default value to the param_list in case one of the values is not found
    #By default it is just filled with np.nan 
    if company != "":
        folder = folder_i+company
    else:
        company = "database"
    if param_def_list == []:
        param_def_list = [np.nan for par in param_list]
    elif len(param_def_list) != len(param_list):
        raise Exception("Please use a full default parameter list or don't use it at all") 
    param_list = [i.lower() for i in param_list]
    os.chdir(folder)
    list_df = []
    if (len(glob.glob("*.xml")) < 1):
        for f in glob.glob("*"):
            print("removing ", f)
            os.remove(folder+'/'+f)
        try:
            os.chdir(folder_i)
            print("and directory ",folder)
            os.rmdir(folder)
        except OSError as e:
            print("Error: %s : %s" % (folder, e.strerror))
        return
    for file in glob.glob("*.xml"): 
        fillings = lh.parse(file) 
        results=[]
        c = 0
        for param in param_list:   
            params = fillings.xpath('//'+param) 
            #print(params)
            if len(params) == 1:                  
                results.append(params[0].text)
            elif len(params) == 0:
                results.append(param_def_list[c])
            else:
                print("Unexpected multiple instances of ",param, 'in file ', file) #Optional parameters should only have one instance per file, which is why this is checked
                print("This may be normal for reporting person data in joint fillings\n")
                results.append(params[0].text)
            c = c+1      
        holdings = fillings.findall('//'+'nonderivativeholding')
        total_hold = 0.0
        for holding in holdings: #Holding sum
            try:
                holdin=holding.find('posttransactionamounts')
                try:
                    holdi=holdin.find('sharesownedfollowingtransaction')
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

        stock = fillings.findall('//nonderivativetransaction')  
        #derivatives = fillings.find('//derivativeTable')
        derivatives = fillings.findall('//derivativetransaction')

        for shares in stock: #Non-derivative transaction parser
            
            data = [np.nan,np.nan,np.nan,np.nan,np.nan]
            try:
                dates=shares.find('transactiondate')
                share = shares.find('transactionamounts')
                coding = shares.find('transactioncoding')
                post = shares.find('posttransactionamounts')        
                try:
                    code = coding.find('transactioncode')
                    postt = post.find('sharesownedfollowingtransaction')
                    if postt == None and None != post.find('valueownedfollowingtransaction'):
                        #Means it is written in USD instead of shares, which is weird
                        posttt=code
                        posttt.text = 'NaN' ##FOr now NaN until we see if it is worth to include
                        print("Post-transaction amounts written in USD instead of shares. Recommend Check case by case")
                    else:
                        posttt = postt.find('value')
                    date=dates.find('value')
                    sharn=share.find('transactionshares')
                    nshares=sharn.find('value')
                    sharevalue = share.find('transactionpricepershare')
                    shareval = sharevalue.find('value')
                    if shareval is None:
                        shareval = sharevalue.find('footnoteid')
                        #print(shareval.attrib['id'])
                        footnotes = fillings.findall('//footnote')
                        for foot in footnotes:
                            if foot.attrib['id'] == shareval.attrib['id']:
                                #print(foot.text)
                                shareval = foot.text
                                break
                        tempshareval = shareval
                        shareval = re.search("\d+\.\d+ per share",shareval)
                        if shareval == None and re.search("\d+\.\d+",tempshareval) != None :
                            print(foot.text)
                            shareval = "0.0"#Means it was probably awarded, so it was a printed share
                        elif shareval == None:
                            shareval = "0.0"
                        else:
                            shareval = re.search("\d+\.\d+",shareval.group()).group()
                        tempshareval = shareval
                        shareval = nshares
                        shareval.text = tempshareval #Previous 4 lines make it easier to still use .text at the end, even thought our last result was not an object but a string
                        #sys.exit()        
                    
                    acquiredordisposed=share.find('transactionacquireddisposedcode')
                    acquiredordisposed = acquiredordisposed.find('value')
                    data = [date.text,acquiredordisposed.text,float(nshares.text),float(shareval.text),code.text,posttt.text]
                    #print(data)
                except KeyboardInterrupt:
                    sys.exit()
                #except:
                #    print("Transaction info not found for file"+file)
                #    print(postt.text)
                #    sys.exit()
            except KeyboardInterrupt:
                sys.exit()
            #except:
            #    print("Transaction non derivatives not found for file"+file)
            #    continue
            data = data+[0]+results #0 is for indicating it is non-derivative
            list_df.append(data)
        for shares in derivatives:
             #derivative transaction parser
            try:
                dates=shares.find('transactiondate')
                share = shares.find('transactionamounts')
                coding = shares.find('transactioncoding')
                try:
                    date=dates.find('value')
                    sharn=share.find('transactionshares')
                    nshares=sharn.find('value')
                    sharevalue = share.find('transactionpricepershare')
                    shareval = sharevalue.find('value')                    
                    code = coding.find('transactioncode')
                    #print(date.text + '    ' + nshares.text + '   ' + shareval.text+ '   ' + code.text)
                except KeyboardInterrupt:
                    sys.exit()
                except:
                    print("Transaction derivatives in: ", dates.text, " info not found, for file"+file)
                    continue
            except KeyboardInterrupt:
                sys.exit()
            except:
                print("Transaction derivatives not found for file"+file)
                continue
    table = pd.DataFrame(list_df,columns=(['date','acquiredordisposed','shares','price','code','postransaction','derivative','staticholdings']+param_list))
    #print(table)
    if to_file == True:
        table.to_csv("4f_"+company+".csv", index = None, header=True)
    os.chdir(folder_i)
    return table

def make_all_4f(datafold):
    basic_param = ['issuerCik','issuerTradingSymbol','rptOwnerCik','isDirector','isOfficer','isTenPercentOwner','isOther','officerTitle']
    def_basic_param = [np.nan,'',np.nan,0,0,0,0,'']
    os.chdir(datafold)
    for folder in os.listdir(datafold):
        if (os.path.isdir(os.path.join(datafold, folder))):
            print("Reading: ", folder)
            read_4f(datafold, basic_param,def_basic_param,company = folder,to_file = True)
    print("Goodbye")

#From Here on company data not related to 4F
##########################################################
#########################################################

########################################################
def get_yearly_data(ticker_list = [],site = 'Barrons', ex = 'NDQ'):
    revenues = []
    for ticker in ticker_list:
        if (site == 'Barrons'):
                url = 'https://www.barrons.com/quote/stock/us/xnas/'+ticker+'/financials#incomestatement'
                print(url) 
                #waitingtime = 1
                user_agent= random.choice(user_agent_list)
                #waitingtime = waitingtime + 1
                #time.sleep(waitingtime)
                #("Got detected as bot network, waiting to try again... \n")
                page = requests.get(url, headers={"User-Agent":user_agent})
                tables = pd.read_html(page.content)  
                #print(tables)
                time.sleep(0.1)          
                table_f_temp = tables[1]
                print(table_f_temp)
                revenues.append(table_f_temp.iloc[0].to_list())
                if 'thousands' in list(table_f_temp.columns)[0].lower(): #We use last element to know the unit, since it sould be a NaN anyway
                    (revenues[-1])[-1] = 1000
                elif 'millions' in list(table_f_temp.columns)[0].lower():
                    (revenues[-1])[-1] = 1000000
    for i in range(len(revenues)):
        revenues[i] = [ r.replace('(','-').replace(')','').replace('+','') if type(r).__name__ == "str" else r for r in revenues[i]]
    print(revenues)

#list_of_param
def run():
    wd=os.getcwd()
    folder = "C:/Users/csrak/Desktop/python/RIVING-Tools/Data/US/4F/"
    #ticker_list = ['KO',234]
    ticker_list=nasdaq_list(wd)
    a = ticker_list.index("jd")
    ticker_list=ticker_list[a:]
    #get_4f(wd, ticker_list, dfrom = '2014-01-01' ,duntil = '2017-01-01')
    get_4f(wd, ticker_list, dfrom = '2017-01-01')
    #get_4f(wd, 'tsla', number = 800)
    #read_4f(folder, basic_param,def_basic_param,company = "AMZN",to_file = True)
    make_all_4f(folder)
'''    
run()
folder = "C:/Users/csrak/Desktop/python/RIVING-Tools/Data/US/4F/"
make_all_4f(folder)
'''
#get_yearly_data(ticker_list=['tsla'])
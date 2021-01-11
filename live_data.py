import requests
import random
import time
import lxml.html as lh
from time import sleep
import argparse
import pandas as pd
import datetime
import os
import glob
from bs4 import BeautifulSoup
from pathlib import Path
import re
import tabula #install tabula-py
import Chile_Data as CL
from zipfile import ZipFile
import urllib3
import urllib.request as newreq
import numpy as np
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_url_bychunks(url):
    n = 5
    request = newreq.Request(url, headers={'User-Agent' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"})
    request.get_method = lambda : 'HEAD'
    r = newreq.urlopen(request)

    # Verify that the server supports Range requests
    try:
        assert r.headers.get('Accept-Ranges', '') == 'bytes', 'Range requests not supported'
    except:
        return "Not Found"

    # Compute chunk size using a double negation for ceiling division
    total_size = int(r.headers.get('Content-Length'))
    chunk_size = -(-total_size // n)

    # Showing chunked downloads.  This should be run in multiple threads.
    chunks = []
    for i in range(n):
        start = i * chunk_size
        end = start + chunk_size - 1  # Bytes ranges are inclusive
        headers = dict(Range = 'bytes=%d-%d' % (start, end))
        request = newreq.Request(url, headers=headers)
        chunk = newreq.urlopen(request).read()
        chunks.append(chunk)
    print("Chunk worked")
    return request



def USDtoCLPfunc(month,year,day = '01'):
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = 'https://www.exchangerates.org.uk/USD-CLP-'+day+'_'+month+'_'+year+'-exchange-rate-history.html'
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    values = parser.xpath('//div[contains(@class,"large-12 medium-12 small-12 columns nolpad")]//text()')
    if not values:
        return np.nan
    usdtoclp = str(values[3]).replace("Close: 1 USD = ","")
    usdtoclp = usdtoclp.replace(" CLP","")
    usdtoclp=float(usdtoclp)
    return usdtoclp

def mw_quote_CL(ticker): #Obtain quote from marketwatch finance
    ticker=ticker
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = 'https://www.marketwatch.com/investing/stock/' + ticker+'?countrycode=cl'
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('//bg-quote[contains(@class,"value")]//text()')
    if not summary_table:
        quote=np.nan
    else:
        quote=float(summary_table[0].replace(',',''))     
    return quote

def yahoo_quote_CL(ticker): #Obtain quote from yahoo finance
    ticker2=ticker.replace(' ','')+'.SN'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker2,ticker2)
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('//span[contains(@class,"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)")]//text()')
    if not summary_table:
       quote=np.nan
    else:
        quote=float(summary_table[0].replace(',',''))
    return quote
def yahoo_quoteA_CL(ticker): #Obtain quote from yahoo finance
    ticker2=ticker.replace(' ','')+'-A.SN'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker2,ticker2)
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('//span[contains(@class,"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)")]//text()')   
    if not summary_table:
        quote=np.nan
    else:
        quote=float(summary_table[0].replace(',',''))     
    return quote

def barron_quoteA_CL(ticker): #Obtain quote from yahoo finance
    ticker2=ticker+'.a'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "https://www.barrons.com/quote/stock/cl/xsgo/"+ticker2
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('.//td')   
    multiplier='Not Found'
    quote=''
    for i in range(len(summary_table)):
        if summary_table[i].text== 'Market Value':
            multiplier=(summary_table[i+1].text)[-1]
            summary_table=((((summary_table[i+1].text).replace('$','')).replace('B','')).replace('M','')).replace('T','')
            break    
    if multiplier=='Not Found':
        quote=np.nan       
        market_cap=0
    else:
        if multiplier=='T':
                multiplier=1000000000000
        elif multiplier=='B':
            multiplier=1000000000
        elif multiplier=='M':
            multiplier=1000000
        else:
            multiplier=1
        market_cap=float(summary_table)*multiplier
        price = parser.xpath('//span[contains(@class,"market__price bgLast")]//text()')   
        quote=float(price[0].replace(',',''))
    return quote,market_cap

def barron_quoteB_CL(ticker): #Obtain quote from yahoo finance
    ticker2=ticker+'.b'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "https://www.barrons.com/quote/stock/cl/xsgo/"+ticker2
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('.//td')   
    multiplier='Not Found'
    quote=''
    for i in range(len(summary_table)):
        if summary_table[i].text== 'Market Value':
            multiplier=(summary_table[i+1].text)[-1]
            summary_table=((((summary_table[i+1].text).replace('$','')).replace('B','')).replace('M','')).replace('T','')
            break    
    if multiplier=='Not Found':
        quote=np.nan       
        market_cap=0
    else:
        if multiplier=='T':
                multiplier=1000000000000
        elif multiplier=='B':
            multiplier=1000000000
        elif multiplier=='M':
            multiplier=1000000
        else:
            multiplier=1
        market_cap=float(summary_table)*multiplier
        price = parser.xpath('//span[contains(@class,"market__price bgLast")]//text()')   
        quote=float(price[0].replace(',',''))
    return quote,market_cap
    
def barron_quote_CL(ticker): #Obtain quote from yahoo finance
    ticker=ticker.replace(' ','')
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "https://www.barrons.com/quote/stock/cl/xsgo/"+ticker
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('.//td')  
    multiplier='Not Found'
    for i in range(len(summary_table)):
        if summary_table[i].text== 'Market Value':
            multiplier=(summary_table[i+1].text)[-1]
            summary_table=(((((summary_table[i+1].text).replace('$','')).replace('B','')).replace('M','')).replace('T','')).replace(',','')
            break    
    if multiplier=='Not Found':
        quote=np.nan
        market_cap=0
    else: 
        if multiplier=='T':
            multiplier=1000000000000
        elif multiplier=='B':
            multiplier=1000000000
        elif multiplier=='M':
            multiplier=1000000
        else:
            multiplier=1
        market_cap=float(summary_table)*multiplier
        price = parser.xpath('//span[contains(@class,"market__price bgLast")]//text()')   
        quote=float(price[0].replace(',',''))
    return quote,market_cap


def live_quote_cl(ticker):
    quote,market_cap=barron_quote_CL(ticker)
    if quote!=quote or market_cap==0:
        quote,market_cap=barron_quoteB_CL(ticker)
    if quote!=quote or market_cap==0:
        quote,market_cap=barron_quoteA_CL(ticker)
    if quote!=quote or quote==0:
        quote=yahoo_quote_CL(ticker)
    if quote!=quote or quote==0:
        quote=yahoo_quoteA_CL(ticker)
    if quote!=quote or quote==0:
        quote=mw_quote_CL(ticker)
    if market_cap==0:
        market_cap=np.nan
    return float(quote),float(market_cap)

        #return quote
#print(mw_quote_CL('SMU'))    
#print(bloomberg_quote_CL('ANDACOR'))
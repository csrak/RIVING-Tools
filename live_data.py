import requests
import random
import time
import lxml.html as lh
from time import sleep
import argparse
import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
from pathlib import Path
import re
import tabula #install tabula-py
import Chile_Data as CL
from zipfile import ZipFile
import urllib3
import numpy as np
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
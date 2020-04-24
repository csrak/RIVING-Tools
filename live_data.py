import requests
import lxml.html as lh
from time import sleep
import argparse
import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
from pathlib import Path
from difflib import SequenceMatcher
import re
import tabula #install tabula-py
import Chile_Data as CL
from zipfile import ZipFile
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def yahoo_quote_CL(ticker): #Obtain quote from yahoo finance
    ticker2=ticker+'.SN'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker2,ticker2)
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    sleep(4)
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('//span[contains(@class,"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)")]//text()')
    if not summary_table:
        quote=yahoo_quoteA_CL(ticker)
    else:
        quote=float(summary_table[0].replace(',',''))
    return quote
def yahoo_quoteA_CL(ticker): #Obtain quote from yahoo finance
    ticker=ticker+'-A.SN'
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker,ticker)
    response = requests.get(url, headers=agent,verify=False)
    print ("Parsing %s"%(url))
    sleep(4)
    parser = lh.fromstring(response.text)
    summary_table = parser.xpath('//span[contains(@class,"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)")]//text()')   
    if not summary_table:
        quote='please create bloomberg or marketwatch scraper'
    else:
        quote=float(summary_table[0].replace(',',''))     
    return quote

#print(yahoo_quote_CL('SMU'))
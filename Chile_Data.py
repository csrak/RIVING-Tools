import requests
import lxml.html as lh
import pandas as pd
import os
from pathlib import Path
#from bs4 import BeautifulSoup

#imports data from chilean stocks, into csv file into /data/chile/ folder

month='03'
year='2019'


#Download function list not working/not used yet
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



def scrape_list(month,year):
    #http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs=03&aa_ifrs=2019 For first 3 months of 2019
    template='http://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs=' #For 2019

    year=year
    url=template+month+'&aa_ifrs='+year #Create a handle, page, to handle the contents of the website
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
    return df
 

# 
# 
#
def scrap_filling(companies_list,n,month, year):
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



def read_data(filename,datafold):
    wd=os.getcwd()
    if os.path.exists(wd+datafold+filename):
        df=pd.read_csv(wd+datafold+filename, header=0)
    else:
        print('File not found in '+ wd+datafold+filename)
        return 0
    
    return df


file_name='registered_stocks_'+month+'-'+year+'.csv'
datafold='\\Data\\Chile\\'
df=read_data(file_name,datafold)
print(df.head())
print(df.shape[1])

#df=scrape_list(month,year)
#scrap_filling(df,n,month, year)
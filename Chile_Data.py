import requests
import sys
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
import tabula  # install tabula-py
import PyPDF2
import unidecode
from datetime import datetime
import shutil

# Set root_dir to the directory where the script is located
root_dir = Path(__file__).resolve().parent

# Set datafold as a Path object relative to root_dir
datafold = root_dir / 'Data' / 'Chile'
print(datafold)
# User agent for web requests
agent = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
}

MAXMONTHS = 100  # Number maximum of empty urls in between monthly fillings

# Function to get the index positions of a value in a DataFrame
def getIndexes(dfObj, value):
    listOfPos = list()
    result = dfObj.isin([value])
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append((row, col))
    return listOfPos

# Function to scrape company links from CMF website
def scrap_company_Links(companies_list, month, year):
    url = f'https://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs={month}&aa_ifrs={year}'
    out = ['Not Found'] * companies_list.shape[0]
    found = 0
    try:
        page = requests.get(url, headers=agent)
        page = lh.fromstring(page.content)
    except requests.exceptions.ConnectionError:
        print("Connection error -> Trying next one")
        return 'Invalid Link'
    except:
        print(f"Error:{url}\n  Trying again")
        for i in range(1, 10):
            if found != 0:
                break
            elif i >= 15:
                return out
            try:
                page = requests.get(url, headers=agent)
                page = lh.fromstring(page.content)
                found = 1
            except requests.exceptions.ConnectionError:
                print("Connection error -> Trying next one")
                return 'Invalid Link'
            except KeyboardInterrupt:
                print('Interrupted')
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except:
                found = 0
                print(f"Error:{url}\n  Trying again")
    link = page.xpath('//a')
    for link in link:
        for i in range(companies_list.shape[0]):
            rut = companies_list.loc[i, 'RUT']
            if 'href' in link.attrib and rut in link.attrib['href']:
                out[i] = link.attrib['href']
                out[i] = f'https://www.cmfchile.cl/institucional/mercados/{out[i]}'
    return out

# Function to scrape file links (PDF or XBRL) from CMF website
def scrap_file_links(url, filet):
    out = ''
    if filet == 999:
        return 'Invalid Link'
    found = 0
    try:
        page = requests.get(url, headers=agent)
        page = lh.fromstring(page.content)
    except requests.exceptions.ConnectionError:
        print("Connection error -> Trying next one")
        return 'Invalid Link'
    except:
        print(f"Error:{url}\n  Trying again")
        for i in range(1, 10):
            if found != 0:
                break
            elif i >= 15:
                return out, filet
            try:
                page = requests.get(url, headers=agent)
                page = lh.fromstring(page.content)
                found = 1
            except requests.exceptions.ConnectionError:
                print("Connection error -> Trying next one")
                return 'Invalid Link'
            except KeyboardInterrupt:
                print('Interrupted')
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except:
                found = 0
                print(f"Error:{url}\n  Trying again")
    links = page.xpath('//a')
    for link in links:
        if 'href' in link.attrib and 'XBRL' in link.text_content():
            out = link.attrib['href']
            out = f'https://www.cmfchile.cl/institucional{out[2:]}'
            out = re.sub(' ', '%20', out)
            filet = 1
            #print("Works",link.text_content())
        elif 'href' in link.attrib and 'PDF' in link.text_content():
            out = link.attrib['href']
            out = f'https://www.cmfchile.cl/institucional{out[2:]}'
            out = re.sub(' ', '%20', out)
            filet = 0
    return out, filet

# Function to download a list of registered stocks from the CMF website
def download_list(month, year):
    template = 'https://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs_excel2.php?'
    year = f'aa={year}'
    month = f'&mm={month}'
    url = template + year + month
    myfile = requests.get(url)
    file_name = Path(f'Ticker_data/registered_stocks_{month}-{year}.xls')
    open(root_dir / datafold / file_name, 'wb').write(myfile.content)

# Function to generate a URL depending on the site, month, and year
def url_generator(site, month, year):
    url = 'no url found'
    if site in ['cmf', 'cmf.cl']:
        template = 'https://www.cmfchile.cl/institucional/mercados/novedades_envio_sa_ifrs.php?mm_ifrs='
        url = template + month + '&aa_ifrs=' + year
    elif site in ['mwcl', 'MWCL', 'MarketWatchcl', 'Market Watch Chile']:
        url = 'https://www.marketwatch.com/tools/markets/stocks/country/chile'
    return url

# Function to scrape a list from a given URL and return it as a pandas DataFrame
def scrap_lists(url):
    found = 0
    try:
        page = requests.get(url, headers=agent)
    except:
        print(f"Error:{url}\n  Trying again")
        for i in range(1, 10):
            if found != 0:
                break
            elif i >= 9:
                return pd.DataFrame()
            try:
                page = requests.get(url, headers=agent)
                found = 1
            except requests.exceptions.ConnectionError:
                print("Connection error")
                return pd.DataFrame()
            except KeyboardInterrupt:
                print('Interrupted')
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except:
                found = 0
                print(f"Error:{url}\n  Trying again")
    doc = lh.fromstring(page.content)
    tr_elements = doc.xpath('//tr')
    col = []
    i = 0
    for t in tr_elements[0]:
        i += 1
        name = t.text_content()
        col.append((name, []))
    for j in range(1, len(tr_elements)):
        T = tr_elements[j]
        if len(T) != len(tr_elements[1]):
            break
        i = 0
        for t in T.iterchildren():
            data = t.text_content()
            if i > 0:
                try:
                    data = int(data)
                except:
                    pass
            col[i][1].append(data)
            i += 1
    Dict = {title: column for (title, column) in col}
    df = pd.DataFrame(Dict)
    return df

# Function to scrape fillings (in zip format, XBRL inside) from the given URLs
def scrap_fillings(urls, filenames, update=0):
    session = requests.Session()
    for i in range(len(urls)):
        if filenames[i] == '0':
            continue
        else:
            url = urls[i]
            found = 0
            try:
                myfile = session.get(url, headers=agent)
            except:
                print(f"Error:{url}\n  Trying again")
                for i in range(1, 10):
                    if found != 0:
                        break
                    elif i >= 9:
                        return
                    try:
                        myfile = session.get(url, headers=agent)
                        found = 1
                    except KeyboardInterrupt:
                        print('Interrupted')
                        try:
                            sys.exit(0)
                        except SystemExit:
                            os._exit(0)
                    except:
                        found = 0
                        print(f"Error:{url}\n  Trying again")
                        continue
            filename = Path(filenames[i])
            filepath = root_dir / datafold / filename
            if update != 0 or not os.path.exists(filepath):
                with open(filepath, 'wb') as f:
                    f.write(myfile.content)
                temp = str(filepath)
                if temp.endswith('.zip'):
                    temp = temp.replace('.zip', '')
                    if not os.path.exists(temp):
                        os.mkdir(temp)
                    print('Downloading ' + temp + '...\n')
                    downloaded = 1
                    time.sleep(1)
                    while downloaded != 0:
                        try:
                            with ZipFile(filepath, 'r') as zipObj:
                                zipObj.extractall(temp)
                                downloaded = 0
                        except BadZipfile:
                            downloaded += 1
                            print('Still downloading (' + str(downloaded * 5) + ' seconds) ' + temp + ' \n')
                            time.sleep(5)
                        if downloaded > 12:
                            print('Error downloading ' + temp + '. Please download manually before executing "allcompanies"')
                            break
            else:
                print("Already downloaded " + str(filepath) + ' and Update not set')

# Function to read previously saved data from a CSV file into a pandas DataFrame
def read_data(filename, datafolder=None, wd=0, oparse_dates=[]):
    print(filename)
    print(datafolder)

    filename = Path(filename)

    # Check if datafolder is absolute, if so, make it relative to root_dir
    if datafolder is not None:
        datafolder = Path(datafolder)
        if datafolder.is_absolute():
            raise ValueError("The datafolder path should be relative, not absolute.")
        filepath = root_dir / datafolder / filename
    else:
        filepath = root_dir / datafold / filename

    # Check if the file exists
    if os.path.exists(filepath):
        if not oparse_dates:
            df = pd.read_csv(filepath, header=0)
            df = df.astype(str)
        else:
            df = pd.read_csv(filepath, header=0, parse_dates=oparse_dates)
            df = df.astype(str)
    else:
        raise ValueError('File not found in ' + str(filepath))

    return df


# Function to scrape RUT lists from the CMF website
def scrap_rutlist(month, year):
    url = url_generator('cmf', month, year)
    df = scrap_lists(url)
    df = df.drop(["Fecha Primer envío", "Fecha último envío", "Tipo Balance"], axis=1)
    print(df.head())
    file_name = Path(f'Ticker_data/registered_stocks_{month}-{year}.csv')
    df.to_csv(root_dir / datafold / file_name, index=None, header=True)

# Function to scrape the list of companies in the Santiago market from MarketWatch Chile
def scrap_mw():
    url = url_generator('mwcl', '0', '0')
    df = scrap_lists(url)
    df = df.drop("Exchange", axis=1)
    print(df.head())
    tickers = []
    for i in range(df.shape[0]):  # Here we extract tickers from the name list
        tickers.append(df.loc[i, 'Name'])
        pos1 = tickers[i].find('(')
        pos2 = tickers[i].find(')')
        tickers[i] = tickers[i][pos1 + 1:pos2]
        pos1 = pos2 - pos1 + 1
        df.loc[i, 'Name'] = (df.loc[i, 'Name'])[:-pos1]
    df['Ticker'] = tickers
    file_name = Path('registered_stocks.csv')
    df.to_csv(root_dir / datafold / file_name, index=None, header=True)

# Function to scrape tickers from a list of tickers
def scrap_tickers(offline=False):
    file_name = Path('list_of_tickers.pdf')
    if not offline:
        url = ('http://cibe.bolsadesantiago.com/EmisoresyValores/Nminas%20Emisores/1.%20N%C3%B3mina%20Emisores%20de%20Acciones.pdf')
        print(url)
        response = requests.get(url)
        with open(root_dir / datafold / file_name, 'wb') as f:
            print("Please wait until new list of public companies is downloaded")
            f.write(response.content)
            time.sleep(10)
    with open(root_dir / datafold / file_name, 'rb') as filepdf:
        filling = PyPDF2.PdfFileReader(filepdf)
        table = []
        for i in range(filling.numPages):
            page = filling.getPage(i)
            temp = page.extractText()
            table.append(temp.split('\n'))
    tickers = ['Ticker']
    rut = ['RUT']
    razon = ['Name']
    count = 1
    companies_full = []
    for pages in table:
        pages = [value for value in pages if value != '']
        companies = []
        while True:
            found_companies = [i for i in pages if i.startswith(str(count))]
            if len(found_companies) == 0:
                break
            for ix, comp in enumerate(found_companies):
                length = len(str(count))
                if comp[length].isnumeric():
                    continue
                else:
                    companies.append(comp)
                    count += 1
        for ix, comp in enumerate(companies):
            if ix != len(companies) - 1:
                indexs = pages.index(comp)
                indexf = pages.index(companies[ix + 1])
                temp_elem = []
                for ins in range(indexs, indexf):
                    temp_elem.append(pages[ins])
                temp_elem = ' '.join(temp_elem).lstrip('0123456789.- ')
                temp_elem = temp_elem.split(" ")
            companies_full.append(temp_elem)
    for comp in companies_full:
        rut.append(comp[-1].replace('.', ''))
        tickers.append(comp[0])
        count = 0
        while "-" in comp[count]:
            count += 1
        razon.append(' '.join(comp[count:-2]))
        if "-BA" in comp[1]:
            rut.append(comp[-1].replace('.', ''))
            tickers.append(comp[0].replace("-A", "-B"))
            razon.append(' '.join(comp[count:-2]))
    final_list = [tickers, rut, razon]
    temp_final_list = []
    for j in range(len(tickers)):
        dim = []
        for i in range(len(final_list)):
            dim.append(unidecode.unidecode(final_list[i][j]))
        temp_final_list.append(dim)
    final_list = temp_final_list
    column_names = final_list.pop(0)
    df = pd.DataFrame(final_list, columns=column_names)
    file_name = Path('registered_stocks.csv')
    df.to_csv(root_dir / datafold / file_name, index=None, header=True)

# Function to obtain RUTs from a provided list and format them for URL scraping
def get_ruts(df):
    if not isinstance(df, int):
        for i in range(df.shape[0]):
            df.loc[i, 'RUT'] = (df.loc[i, 'RUT'])[:-2]
    else:
        raise SystemExit('\nPlease Update not setting "Scrap" argument in "upandgetem" function ')
    return df

# Function to map tickers to RUTs and determine the most similar name using SequenceMatcher
def Tick2Rut(ruts, tickers):
    rut_order = []
    file_order = []
    curr_simil = 0
    range1 = tickers.shape[0]
    range2 = ruts.shape[0]
    for i in range(range1):
        rut_order.append('0')
        file_order.append('0')
        for j in range(range2):
            a = tickers.loc[i, 'Name']
            b = ruts.loc[j, 'Razón social']
            b = b.upper()
            a = a.upper()
            b = re.sub('Ñ', 'N', b)
            a = re.sub('Ñ', 'N', a)
            simil = SequenceMatcher(None, a, b).ratio()
            if simil > curr_simil:
                rut_order[i] = ruts.loc[j, 'Rut']
                file_order[i] = ruts.loc[j, 'Tipo Envio']
                curr_simil = simil
            if j == (range2 - 1) and curr_simil < 0.90:
                rut_order[i] = 'ERROR HIGH: BANK OR NOT FOUND'
    tickers['Rut'] = rut_order
    tickers['File'] = file_order
    return tickers

# Function to search for tickers corresponding to banks and obtain the IFI Code of each
def tick2code(datafold, names):
    df = read_data('registered_stocks.csv', wd=1)
    tickers = [df['Ticker'].tolist(), df['Name'].tolist()]
    for i in range(len(names[0])):
        for j in range(len(tickers[0])):
            if SequenceMatcher(None, tickers[1][j].lower(), names[0][i].lower()).ratio() > 0.8:
                names[0][i] = tickers[0][j]
                break
    return names

# Function to brute-force scrape bank data from the SBIF website
def bruteforce_bank_scrap(month, year, month2='03', year2='2020', update=0):
    if int(str(datetime.now())[0:4]) < int(year2) or (int(str(datetime.now())[0:4]) == int(year2) and int(str(datetime.now())[5:7]) <= (int(month2) - 2)):
        print('The date solicited is too recent, please put another date or download the last month manually')
        return
    month = int(month)
    year = int(year)
    month2 = int(month2)
    year2 = int(year2)
    previous_months = (2020 - year) * 12 - (month) + 4
    next_months = (year2 - 2020) * 12 + (month2) - 3
    datafold = root_dir / 'Data' / 'Chile' / 'Banks'
    if not os.path.exists(root_dir / datafold):
        os.mkdir(root_dir / datafold)
    i = 0
    number_of_files = 0
    name_month = 3
    name_year = 2020
    session = requests.Session()
    while number_of_files < previous_months and i < MAXMONTHS * previous_months:
        url = f'https://www.sbif.cl/sbifweb3/internet/archivos/Info_Fin_7877_{str(19022 - i)}.zip'
        i += 1
        stname_month = f'{name_month:02}'
        filename = Path(f'Banks_{stname_month}-{name_year}')
        filepath = root_dir / datafold / filename
        if update == 0 or not os.path.exists(filepath):
            myfile = session.get(url, headers=agent)
            page = lh.fromstring(myfile.content)
            test = page.xpath('//h1[contains(@class,"titulo")]')
            if not test:
                with open(filepath.with_suffix('.zip'), 'wb') as f:
                    f.write(myfile.content)
                print('Downloading ' + str(filepath) + '...\n')
                downloaded = 1
                time.sleep(1)
                while downloaded != 0:
                    try:
                        with ZipFile(filepath.with_suffix('.zip'), 'r') as zipObj:
                            if not os.path.exists(filepath):
                                os.mkdir(filepath)
                            zipObj.extractall(filepath)
                            if glob.glob(str(filepath) + f'/{name_year}{stname_month}*'):
                                number_of_files += 1
                                if name_month > 1:
                                    name_month -= 1
                                else:
                                    name_month = 12
                                    name_year -= 1
                                strip_spaces(filepath)
                            else:
                                shutil.rmtree(filepath)
                            downloaded = 0
                    except BadZipfile:
                        downloaded += 1
                        print('Still downloading (' + str(downloaded) + ' seconds) ' + str(filepath) + '.zip\n')
                        time.sleep(5)
                    if downloaded > 12:
                        print('Error downloading ' + str(filepath) + '.zip. Please download manually before executing "allcompanies"')
                        break
        else:
            print(f"Already downloaded {filepath} and Update not set")
            if name_month > 1:
                name_month -= 1
            else:
                name_month = 12
                name_year -= 1
    number_of_files = 0
    name_month = 4
    name_year = 2020
    i = 1
    while number_of_files < next_months and i < MAXMONTHS * next_months:
        url = f'https://www.sbif.cl/sbifweb3/internet/archivos/Info_Fin_7877_{str(19022 + i)}.zip'
        i += 1
        stname_month = f'{name_month:02}'
        filename = Path(f'Banks_{stname_month}-{name_year}')
        filepath = root_dir / datafold / filename
        if update == 0 or not os.path.exists(filepath.with_suffix('.zip')):
            myfile = session.get(url, headers=agent)
            page = lh.fromstring(myfile.content)
            test = page.xpath('//h1[contains(@class,"titulo")]')
            if not test:
                with open(filepath.with_suffix('.zip'), 'wb') as f:
                    f.write(myfile.content)
                print('Downloading ' + str(filepath) + '...\n')
                downloaded = 1
                time.sleep(1)
                while downloaded != 0:
                    try:
                        with ZipFile(filepath.with_suffix('.zip'), 'r') as zipObj:
                            if not os.path.exists(filepath):
                                os.mkdir(filepath)
                            zipObj.extractall(filepath)
                            if glob.glob(str(filepath) + f'/{name_year}{stname_month}*'):
                                number_of_files += 1
                                if name_month < 12:
                                    name_month += 1
                                else:
                                    name_month = 1
                                    name_year += 1
                                strip_spaces(filepath)
                            else:
                                shutil.rmtree(filepath)
                            downloaded = 0
                    except BadZipfile:
                        downloaded += 1
                        print('Still downloading (' + str(downloaded) + ' seconds) ' + str(filepath) + '.zip\n')
                        time.sleep(5)
                    if downloaded > 12:
                        print('Error downloading ' + str(filepath) + '.zip. Please download manually before executing "allcompanies"')
                        break
        else:
            print(f"Already downloaded {filepath}.zip and Update not set")
            if name_month < 12:
                name_month += 1
            else:
                name_month = 1
                name_year += 1
    print('Finished Download of bank data')

# Function to read bank codes from given files
def read_bank_codes(datafold):
    try:
        df = pd.read_csv(root_dir / datafold / 'Instrucciones/CODIFIS.TXT', skiprows=3, delimiter='\t', index_col=False, encoding='latin1')
    except pd.errors.ParserError:
        df = pd.read_csv(root_dir / 'Data' / 'Chile' / 'Banks' / 'Banks_03-2020' / '202003-290420' / 'Instrucciones' / 'CODIFIS.TXT', skiprows=3, delimiter='\t', index_col=False, encoding='latin1')
    except FileNotFoundError:
        df = pd.read_csv(root_dir / datafold / 'Instrucciones' / 'Instrucciones' / 'CODIFIS.TXT', skiprows=3, delimiter='\t', index_col=False, encoding='latin1')
    codes = df['COD. IFI'].tolist()
    names = df['RAZON SOCIAL'].tolist()
    names = [(name.replace('(', '')).replace(')', '') for name in names if name == name]
    for i in range(len(names)):
        numbinnames = [s for s in names[i].split() if s.isdigit()]
        for numb in numbinnames:
            names[i] = names[i].replace(numb, '')
    codes = codes[:len(names)]
    codes.pop(-1)
    names.pop(-1)
    names = [names, codes]
    df = pd.read_csv(root_dir / datafold / 'Instrucciones' / 'Modelo-MB1.TXT', skiprows=1, delimiter='\t', header=0, names=['code', 'parameter'], usecols=['code', 'parameter'], index_col=False, encoding='latin1')
    codes = df['code'].tolist()
    parameter = df['parameter'].tolist()
    n = len(parameter)
    for j in range(n):
        i = n - 1 - j
        if parameter[i] == parameter[i] and codes[i] == codes[i]:
            parameter[i] = parameter[i].strip()
            codes[i] = str(codes[i]).strip()
        else:
            parameter.pop(i)
            codes.pop(i)
    parameter = [unidecode.unidecode(((para.replace('(', '')).replace(')', '')).replace('*', '')) for para in parameter]
    codes = [code.strip() for code in codes]
    mb1 = [parameter, codes]
    df = pd.read_csv(root_dir / datafold / 'Instrucciones' / 'Modelo-MR1.TXT', skiprows=1, delimiter='\t', header=0, names=['code', 'parameter'], usecols=['code', 'parameter'], index_col=False, encoding='latin1')
    codes = df['code'].tolist()
    parameter = df['parameter'].tolist()
    n = len(parameter)
    for j in range(n):
        i = n - 1 - j
        if parameter[i] == parameter[i] and codes[i] == codes[i]:
            parameter[i] = parameter[i].strip()
            codes[i] = str(codes[i]).strip()
        else:
            parameter.pop(i)
            codes.pop(i)
    parameter = [unidecode.unidecode(((para.replace('(', '')).replace(')', '')).replace('*', '')) for para in parameter]
    codes = [code.strip() for code in codes]
    mr1 = [parameter, codes]
    df = pd.read_csv(root_dir / datafold / 'Instrucciones' / 'Modelo-MC1.TXT', skiprows=1, delimiter='\t', header=0, names=['code', 'parameter'], usecols=['code', 'parameter'], index_col=False, encoding='latin1')
    codes = df['code'].tolist()
    parameter = df['parameter'].tolist()
    n = len(parameter)
    for j in range(n):
        i = n - 1 - j
        if parameter[i] == parameter[i] and codes[i] == codes[i]:
            parameter[i] = parameter[i].strip()
            codes[i] = str(codes[i]).strip()
        else:
            parameter.pop(i)
            codes.pop(i)
    parameter = [unidecode.unidecode(((para.replace('(', '')).replace(')', '')).replace('*', '')) for para in parameter]
    codes = [code.strip() for code in codes]
    mc1 = [parameter, codes]
    return names, mb1, mr1, mc1

# Function to get bank tickers and corresponding codes
def get_bank_tickers(datafold):
    names, mb1, mr1, mc1 = read_bank_codes(datafold)
    names = tick2code(datafold, names)
    return names, mb1, mr1, mc1

# Function to remove extra spaces from all .txt files in a directory
def strip_spaces(datafold):
    print('Formating files ...')
    for files in glob.glob(str(root_dir / datafold / "**/*.txt"), recursive=True):
        clean_lines = []
        with open(files, "r", encoding='latin1') as f:
            lines = f.readlines()
            clean_lines = [(l.strip()).strip('\t') for l in lines if l.strip()]
        with open(files, "w", encoding='latin1') as f:
            f.writelines('\n'.join(clean_lines))

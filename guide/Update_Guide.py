# import packages
import requests
import json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import urllib
# from tastypie import http
import http.client
from pandas import Series, DataFrame
import time
# import matplotlib.pyplot as plt
from random import randint
from time import sleep
import time, random, requests
from uszipcode import SearchEngine

ZipSearch = SearchEngine(simple_zipcode=True)  # set simple_zipcode=False to use rich info database
import math
import time
from multiprocessing import set_start_method, get_context, Pool
from TorRequest import *
import requests
import re

# used: stkTypId=28881
# New: stkTypId=28880
# CPO: cpoId=28444
# https://www.cars.com/for-sale/searchresults.action/?clrId=27122%2C27123%2C27124%2C27125%2C27126%2C27127%2C27128%2C27129%2C27131%2C27132%2C27133%2C27134&page=1&perPage=100&rd=10&searchSource=GN_BREADCRUMB&sort=price-highest&zc=85281

zip_code = 85281


def get_radius(zip_code):
    print(zip_code)
    return ZipSearch.by_zipcode(zip_code).radius_in_miles


def Evaluate(URL, session):
    header = {'User-Agent': 'Mozilla/6.0'}
    try:
        html, session, _ = TorRequest(URL, balanced_ip_use=True, session=session)
    except (http.client.IncompleteRead) as e:
        page = e.partial
    soup = BeautifulSoup(html, 'lxml')

    Listing_Count = soup.find("span", {"class": "filter-count"}).text.replace(',', '')
    if Listing_Count == 'No':
        Listing_Count = 0
        Max_Pages = 0
    else:
        Listing_Count = int(Listing_Count)
        pagination_list = soup.find("ul", {"class": "page-list"}).find_all("a")
        Max_Pages = int(max([x.string for x in pagination_list]))

    Pages = list(range(1, Max_Pages + 1))
    random.shuffle(Pages)

    # print(f'Max_Pages: {Max_Pages}, Listing_Count: {Listing_Count}, URL: {URL}')
    return Listing_Count, Max_Pages, Pages, session


######
base_url = 'https://www.cars.com/for-sale/searchresults.action/?page=1&perPage=100&searchSource=GN_REFINEMENT&slrTypeId=28878&sort=relevance'


def Shuffle_Pages(max_pages):
    Shuffled_Pages = list(range(1, max_pages + 1))
    random.shuffle(Shuffled_Pages)
    # print(f'Page order: {Shuffled_Pages}')
    return Shuffled_Pages


def Add_Pages():
    Guide = pd.read_csv('./guide/Guide_Updated_Merged.csv')
    '''create one new line for each page'''
    Guide_With_Pages = pd.DataFrame()
    for i in Guide.index:
        Max_Pages = Guide.loc[i, 'Max_Pages']
        Shuffled_Pages = Shuffle_Pages(Max_Pages)
        print(f'{i}- Pages: {Max_Pages}')
        URL_End = Guide.loc[i, 'URL_end']

        for p in Shuffled_Pages:
            new_row = Guide[i:i + 1].copy()
            new_row.loc[i, 'URL_end'] = f'&page={p}{URL_End}'  # Add Page number
            new_row.loc[i, 'Page'] = p
            Guide_With_Pages = Guide_With_Pages.append(new_row)

    Guide_With_Pages = Guide_With_Pages[['zipcode', 'URL_end', 'Page']]
    Guide_With_Pages.to_csv('./guide/Guide_With_Pages.csv', index=0)
    return Guide_With_Pages


def replace_radius(url):
    p = .25  # percent reduction in radius to avoid overlap
    r = re.search("rd=([\d]+)", url)[1]
    r_new = round(max(int(r) * (1 - p), 1))
    return url.replace(f'rd={r}', f'rd={r_new}')


def Update_Guide(M):
    Guide = pd.read_csv('./guide/scrape_guide.csv')
    Guide['URL_end'] = Guide['URL_end'].apply(replace_radius)

    Guide['Listing_Count_old'] = Guide['Listing_Count']
    Guide['Listing_Count'] = ''
    Guide.loc[Guide['Listing_Count_old']<=100, 'Listing_Count'] = Guide['Listing_Count_old']

    N = len(Guide)
    ll = int(N / Threads) * (M - 1)
    uu = min(N, int(N / Threads) * (M))

    print(f'evaluating Guide.index[{ll}:{uu}]')
    # Add Evaluate All URLs
    session = False
    for i in Guide.index[ll:uu]:
        try:
            if Guide.loc[i, 'Listing_Count'] != '':
                continue

            z = str(Guide.loc[i, 'zipcode'])
            if len(z) == 4:
                z = '0' + z

            r = Guide.loc[i, 'radius']

            URL_end = Guide.loc[i, 'URL_end']
            URL = f'{base_url}&{URL_end}'
            Listing_Count, Max_Pages, Pages, session = Evaluate(URL, session)
            Guide.loc[i, 'Listing_Count'], Guide.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            print(f'Thread{M}-{round((i - ll)/(uu-ll)*100)}%  Zip:{z}  #:{Listing_Count}  PP:{Max_Pages} i:{i}')
        except Exception as e:
            print(i, e)

        if (i - ll) % 100 == 0:
            Guide[ll:uu].to_csv(f'./guide/Guide_Updated_chunk{M}.csv')
    Guide[ll:uu].to_csv(f'./guide/Guide_Updated_chunk{M}.csv')


def Update_Guide_Resume(M):
    Guide = pd.read_csv(f'./guide/Guide_Updated_chunk{M}.csv')
    Guide['URL_end'] = Guide['URL_end'].apply(replace_radius)

    N = len(Guide)
    ll = min(Guide.index)
    uu = max(Guide.index)

    print(f'evaluating chunk {M} Guide.index[{ll}:{uu}]')
    # Add Evaluate All URLs
    session = False
    for i in Guide.index[ll:uu]:
        try:
            if Guide.loc[i, 'Listing_Count'] != '':
                continue

            z = str(Guide.loc[i, 'zipcode'])
            if len(z) == 4:
                z = '0' + z

            r = Guide.loc[i, 'radius']

            URL_end = Guide.loc[i, 'URL_end']
            URL = f'{base_url}&{URL_end}'
            Listing_Count, Max_Pages, Pages, session = Evaluate(URL, session)
            Guide.loc[i, 'Listing_Count'], Guide.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            print(f'Thread{M}-{round((i - ll)/(uu-ll)*100)}%  Zip:{z}  #:{Listing_Count}  PP:{Max_Pages} i:{i}')
        except Exception as e:
            print(i, e)

        if (i - ll) % 100 == 0:
            Guide[ll:uu].to_csv(f'./guide/Guide_Updated_chunk{M}.csv')
    Guide[ll:uu].to_csv(f'./guide/Guide_Updated_chunk{M}.csv')


def Update_Guide_inparallel():
    # with get_context("spawn").Pool(Threads) as pool:
    #     pool.map(Update_Guide, range(1, Threads+1))  # 10 parallel threads
    #     pool.close()
    #     pool.join()

    z = pd.read_csv(f'./guide/Guide_Updated_chunk1.csv')
    for M in range(2, Threads+1):
        print(f'reading Guide_Updated_chunk{M}.csv')
        z = z.append(pd.read_csv(f'./guide/Guide_Updated_chunk{M}.csv'))
    z.to_csv('./guide/Guide_Updated_Merged.csv', index=False)

def Update_Guide_Resume_inparallel():
    with get_context("spawn").Pool(Threads) as pool:
        pool.map(Update_Guide_Resume, range(1, Threads+1))  # 10 parallel threads
        pool.close()
        pool.join()

    z = pd.read_csv(f'./guide/Guide_Updated_chunk1.csv')
    for M in range(2, 11):
        z = z.append(pd.read_csv(f'./guide/Guide_Updated_chunk{M}.csv'))
    z.to_csv('./guide/Guide_Updated_Merged.csv', index=False)


Threads = 30
if __name__ == '__main__':


    Update_Guide_inparallel()
    #Update_Guide_Resume_inparallel()

    Add_Pages()


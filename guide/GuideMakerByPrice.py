
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
ZipSearch = SearchEngine(simple_zipcode=True) # set simple_zipcode=False to use rich info database
import math
import time
from multiprocessing import Pool
from TorRequest import *
import requests
import re
from itertools import product
from itertools import repeat
import sys
# used: stkTypId=28881
# New: stkTypId=28880
# CPO: cpoId=28444
# https://www.cars.com/for-sale/searchresults.action/?clrId=27122%2C27123%2C27124%2C27125%2C27126%2C27127%2C27128%2C27129%2C27131%2C27132%2C27133%2C27134&page=1&perPage=100&rd=10&searchSource=GN_BREADCRUMB&sort=price-highest&zc=85281

# url = 'https://www.cars.com/for-sale/searchresults.action/?page=1&perPage=100&searchSource=GN_REFINEMENT&sort=relevance&zc=85281&rd=9999&prMn=2500&prMx=2999'
# url = 'https://www.cars.com/shopping/results/?dealer_id=&list_price_max=500&list_price_min=&makes[]=&maximum_distance=all&mileage_max=&sort=best_match_desc&stock_type=all&year_max=2007&year_min=&zip=85281&page_size=250&page=23'
# url = 'https://www.cars.com/shopping/results/?area_expanded=1&dealer_id=&list_price_max=500&list_price_min=&makes[]=&maximum_distance=all&mileage_max=&page_size=20&sort=best_match_desc&stock_type=all&year_max=2007&year_min=&zip=85281&page=2000'
def Evaluate(url, session=False):
    try:
        # html, session, _ = TorRequest(url, balanced_ip_use=True, session=session)
        html, session, keep_session = RegularRequest(url, session=session)
    except (http.client.IncompleteRead) as e:
        page = e.partial
    soup = BeautifulSoup(html, 'lxml')

    Listing_Count = soup.find("span", {"class": "total-filter-count"})
    if Listing_Count:
        Listing_Count = Listing_Count.text.replace(',','').replace(' thousand','000').replace('Over','').replace(' million','1000000').replace('(','').replace(')','').replace('+','').replace(' matches','').replace(' match','').strip()
    if Listing_Count == '0':
        Listing_Count = 0
    else:
        Listing_Count = int(Listing_Count)
        # pagination_list = soup.find("ul", {"class": "page-list"}).find_all("a")
        # Max_Pages = int(max([int(x.string) for x in pagination_list]))

    Max_Pages = Listing_Count//250+1

    Pages = list(range(1, Max_Pages+1))
    random.shuffle(Pages)

    if Listing_Count > 3000:
        print(f'Max_Pages: {Max_Pages}, \tListing_Count: {Listing_Count}, \tURL: {url}')
    return Listing_Count, Max_Pages, Pages, session

######

def generate_url_df(width=250):

    price_extensions = \
        [f'&list_price_min={i}&list_price_max={i+500-1}' for i in range(0, 3000, 500)] +\
        [f'&list_price_min={i}&list_price_max={i+1000-1}' for i in range(80000, 100000, 1000)] + \
        [f'&list_price_min={i}&list_price_max={i+5000-1}' for i in range(100000, 200000, 5000)] + \
        [f'&list_price_min={200000}']

        # # [f'&list_price_max={50}'] + \
        # [f'&list_price_min={i}&list_price_max={i + 10000 - 1}' for i in range(100000, 200000, 10000)] + \
        # [f'&list_price_min={i}900&list_price_max={i}999' for i in range(12, 36, 1)

    ## Add new price extensions using manual breaks
    breaks = [500, 900, 940, 950, 975, 990, 995, 1000]
    old_j = 0
    for j in breaks:
        s, e = old_j, j
        print(s, e)
        old_j = j
        new_list = [f'&list_price_min={i+s}&list_price_max={i + e - 1}' for i in range(3000, 7000, 1000)]
        price_extensions.extend(new_list)

    ## Add new price extensions using manual breaks
    breaks = [50, 100, 150, 200, 225, 250, 300, 350, 400, 450, 475, 490, 500, 550, 575, 590, 595, 600, 650, 700, 725, 740, 750, 800,
              850, 875, 900, 940, 950, 975, 985, 990, 991, 992, 994, 995, 996, 999, 1000]
    old_j = 0
    for j in breaks:
        s, e = old_j, j
        print(s, e)
        old_j = j
        new_list = [f'&list_price_min={i+s}&list_price_max={i + e - 1}' for i in range(7000, 80000, 1000)]
        price_extensions.extend(new_list)

    random.shuffle(price_extensions)
    price_extensions.sort()


    #url_list = [base_url+p+c for p in price_extensions for c in color_extensions]
    url_list = [base_url+p for p in price_extensions]
    url_list = [(u,) + get_price_bracket(u)  for u in url_list ]

    url_df = pd.DataFrame(url_list, columns=['url', 'min', 'max'])
    url_df = url_df.sort_values(['min'])

    return url_df

# M = 1
def evaluate_urls(M, urls_df):
    urls_df['Listing_Count'] = ''

    N = len(urls_df)
    ll = int(N/10)*(M-1)
    uu = min(N, int(N/10)*(M))

    print(f'evaluating urls_df.index[{ll}:{uu}]')
    # Add Evaluate All URLs
    session = False
    for i in urls_df.index[ll:uu]:
        try:
            # print(f'{M}-{i}')
            if urls_df.loc[i, 'Listing_Count'] != '':
                continue
            url = urls_df.loc[i, 'url']

            Listing_Count, Max_Pages, Pages, session = Evaluate(url, session)
            urls_df.loc[i, 'Listing_Count'], urls_df.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            # print(f'{M}-{(i-ll)}   #:{Listing_Count}  Max_Pages:{Max_Pages}')
        except Exception as e:
            print(i,e)

        if (i-ll) % 100 == 0:
            urls_df[ll:uu].to_csv(f'./guide/GuideByPrice{M}.csv', index=False)
    urls_df[ll:uu].to_csv(f'./guide/GuideByPrice{M}.csv', index=False)


def evaluate_urls_inparallel(urls_df):
    Processes = 15
    pool = Pool()
    pool.starmap(evaluate_urls, zip(range(1, Processes), repeat(urls_df)))  #10 parallel threads
    pool.close()
    pool.join()

    z = pd.read_csv(f'./guide/GuideByPrice1.csv')
    for M in range(2, Processes):
        z = z.append(pd.read_csv(f'./guide/GuideByPrice{M}.csv'))
    return z

def Shuffle_Pages(max_pages):
    Shuffled_Pages = list(range(1, max_pages+1))
    random.shuffle(Shuffled_Pages)
    #print(f'Page order: {Shuffled_Pages}')
    return Shuffled_Pages

def get_price_bracket(url):
    min = max = np.nan
    m1 = re.search(r'list_price_min=(\d*)', url)
    if m1: min = int(m1.groups()[0])
    m2 = re.search(r'list_price_max=(\d*)', url)
    if m2: max = int(m2.groups()[0])
    return min, max

def Compress(Guide):
    '''If listings are too few in a bracket, try to merge with the next/prev'''
    '''For each row, check if it can be combined with next, then merge'''

    Guide = Guide.sort_values(['min'])
    Guide_New = pd.DataFrame(columns=Guide.columns)
    skip_list = []
    for i in Guide.index:
        if i in skip_list:
            continue
        row = Guide[i:i+1]
        new_row = row.copy()
        n1 = Guide.loc[i, 'Listing_Count']
        min1 = int(Guide.loc[i, 'min'])
        try:
            max1 = int(Guide.loc[i, 'max'])
        except:
            max1 = np.nan

        for j in range(i+1, len(Guide)):
            # next bracket
            n2 = Guide.loc[j, 'Listing_Count']
            min2 = int(Guide.loc[j, 'min'])
            try:
                max2 = int(Guide.loc[j, 'max'])
            except:
                max2 = np.nan
            n_sum = new_row.loc[i, 'Listing_Count'] + n2

            # can they be combined?
            if 0 <= n_sum <= 6000:
                # assert max1 == min2 - 1
                # min_new, max_new = min1, max2
                new_row.loc[i, 'url'] = re.sub(r'list_price_max=\d*', rf'list_price_max={str(max2)}',new_row.loc[i, 'url'])
                new_row.loc[i, ['Listing_Count','min', 'max', 'Max_Pages']] = [n_sum, min1, max2, n_sum//250+1]
                print(i, n1, min1, max1, '\t', n2, min2, max2, '\t=>\t',
                      new_row[['Listing_Count', 'min', 'max', 'Max_Pages']].iloc[0].to_list(), 'Combined')
                skip_list.append(j)
            else:
                # print(i, n1, min1, max1, '\t', n2, min2, max2, '\t=>\t', new_row[['Listing_Count', 'min', 'max', 'Max_Pages']].iloc[0].to_list())
                break
        Guide_New = Guide_New.append(new_row)

    Guide_New = Guide_New.reset_index(drop=True)
    Guide_New['guide_id'] = Guide_New.index

    return Guide_New

def Add_Pages(Guide):
    Guide.reset_index(drop=True, inplace=True)
    '''create one new line for each page'''
    Guide_With_Pages = pd.DataFrame()

    for i in Guide.index:
        Max_Pages = Guide.loc[i,'Max_Pages']
        if pd.isna(Max_Pages):
            Max_Pages = 0
        Shuffled_Pages = Shuffle_Pages(int(Max_Pages))
        print(f'{i}- Pages: {Max_Pages}')
        url = Guide.loc[i,'url']
        URL_End = url.replace(base_url, '')

        for p in Shuffled_Pages:
            new_row = Guide[i:i+1].copy()
            URL_End_wPage = f'{URL_End}&page={p}' # Add Page number
            new_row.loc[i, 'url'] = base_url + URL_End_wPage
            new_row.loc[i, 'Page'] = p
            Guide_With_Pages = Guide_With_Pages.append(new_row)

    # Guide_With_Pages = Guide_With_Pages[['zipcode','URL_end','Page']]
    Guide_With_Pages['Result'] = ''
    Guide_With_Pages = Guide_With_Pages.reset_index(drop=True)
    Guide_With_Pages['search_id'] = Guide_With_Pages.index

    Guide_With_Pages.to_csv('./guide/Guide_With_Pages.csv', index=0)
    return Guide_With_Pages



def Break_By_Color(Guide):
    #Break down by color
    color_dict = {'White':['27134'],'Silver':['27133'], 'Gray':['27127'], 'Blue':['27124'], 'Black':['27123'],
                  'Red':['27132'], 'Other': ['27122','27125', '27126', '27128', '27129', '29637', '27131', '27135']}
    color_extensions = ['&clrId=' + '%2C'.join(color) for color in color_dict.values()]

    Guide['oversized'] = Guide['Max_Pages'] >= 49
    resized_urls = []
    for i in Guide[Guide['oversized']].index:
        url = Guide.loc[i, 'url']
        new_urls = [url+f for f in color_extensions]
        resized_urls += new_urls

    urls_df = pd.DataFrame(resized_urls, columns=['url'])
    z = evaluate_urls_inparallel(urls_df, ext='resized_by_color')
    New_Guide = Guide[~Guide['oversized']].append(z)
    return New_Guide

def Break_By_Stock_Type(Guide):
    Guide['oversized'] = Guide['Max_Pages'] >= 45
    Guide.reset_index(drop=True, inplace=True)
    resized_urls = []
    for i in Guide[Guide['oversized']].index:
        url = Guide.loc[i, 'url']
        new = '&stkTypId=28880'
        used = '&stkTypId=28881'
        cpo = '&cpoId=28444'
        stock_extensions = [new,used,cpo]
        new_urls = [url+f for f in stock_extensions]
        resized_urls += new_urls

    urls_df = pd.DataFrame(resized_urls, columns=['url'])
    z = evaluate_urls_inparallel(urls_df, ext='resized_by_stock')
    New_Guide = Guide[~Guide['oversized']].append(z)
    return New_Guide

def Break_By_Price_Badge(Guide):
    Guide['oversized'] = Guide['Max_Pages'] >= 47
    resized_urls = []
    Guide.reset_index(drop=True, inplace=True)
    for i in Guide[Guide['oversized']].index:
        url = Guide.loc[i, 'url']
        price_badge_extensions = ['&carsPriceBadgeV1Id=20160163', '&carsPriceBadgeV1Id=20160164', '&carsPriceBadgeV1Id=C20160048']
        new_urls = [url+f for f in price_badge_extensions]
        resized_urls += new_urls

    urls_df = pd.DataFrame(resized_urls, columns=['url'])
    z = evaluate_urls_inparallel(urls_df, ext='resized_by_price')
    New_Guide = Guide[~Guide['oversized']].append(z)
    return New_Guide


if __name__ == '__main__':
    base_url = 'https://www.cars.com/shopping/results/?page_size=10&searchSource=GN_REFINEMENT&zc=85281&maximum_distance=all'
    urls_df = generate_url_df()
    GuideByPrice_merged = evaluate_urls_inparallel(urls_df) # in future only run up to here to see if any price-bracket is too wide (>10k cars)
    GuideByPrice_merged['url'] = GuideByPrice_merged['url'].replace('page_size=10', 'page_size=250')
    GuideByPrice_merged.to_csv(f'./guide/GuideByPrice_merged.csv', index=False)

    if 'check' in sys.argv:
        print('check complete: see if any price-bracket is too large')

    else:
        print('updating GuideByPrice_merged & Guide_With_Pages')
        GuideByPrice_merged = pd.read_csv(f'./guide/GuideByPrice_merged.csv')

        # Merge small brackets
        Guide = Compress(GuideByPrice_merged)
        Guide.to_csv(f'./guide/Guide.csv', index=False)

        # Guide = pd.read_csv('./guide/Guide.csv')
        # New_Guide = Break_By_Color(Guide)
        # New_Guide1 = Break_By_Price_Badge(New_Guide)
        # New_Guide2 = Break_By_Stock_Type(New_Guide1)
        # New_Guide2.to_csv('./guide/GuideByPrice_merged_resized.csv', index=False
        # Guide_With_Pages = Add_Pages(New_Guide2)

        Guide_With_Pages = Add_Pages(Guide)
        Guide_With_Pages.to_csv('./guide/Guide_With_Pages.csv', index=False)
        print('./guide/Guide_With_Pages.csv saved')


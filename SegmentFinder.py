
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

    Listing_Count = soup.find("span", {"class": "filter-count"}).text.replace(',','')
    if Listing_Count == 'No':
        Listing_Count = 0
        Max_Pages = 0
    else:
        Listing_Count = int(Listing_Count)
        pagination_list = soup.find("ul", {"class": "page-list"}).find_all("a")
        Max_Pages = int(max([x.string for x in pagination_list]))

    Pages = list(range(1, Max_Pages+1))
    random.shuffle(Pages)

    #print(f'Max_Pages: {Max_Pages}, Listing_Count: {Listing_Count}, URL: {URL}')
    return Listing_Count, Max_Pages, Pages, session

######
base_url = 'https://www.cars.com/for-sale/searchresults.action/?page=1&perPage=100&searchSource=GN_REFINEMENT&slrTypeId=28878&sort=relevance'

# Replace Alaska zipcodes with a smaller set of zips with 500 mi radهus
AL_99768 = ['99768', '99627', '99691', '99675', '99757', '99754', '99765', '99748', '99746', '99590', '99777', '99741',
            '99745', '99755', '99756', '99558', '99771', '99684', '99665', '99575', '99668', '99656', '99758', '99667',
            '99557', '99744', '99753', '99767', '99720', '99704', '99773', '99743', '99727', '99602', '99760', '99659',
            '99770', '99739', '99751', '99671', '99709', '99725', '99682', '99774', '99729', '99784', '99688', '99775',
            '99607', '99676', '99701', '99706', '99707', '99708', '99710', '99790', '99703', '99716', '99711', '99694',
            '99683', '99763', '99705', '99652', '99623', '99654', '99687', '99626', '99629', '99657', '99530', '99502',
            '99506', '99726', '99695', '99509', '99510', '99511', '99514', '99519', '99520', '99521', '99522', '99523',
            '99524', '99529', '99653', '99501', '99513', '99517', '99786', '99635', '99599', '99567', '99503', '99508',
            '99505', '99645', '99674', '99702', '99518', '99504', '99585', '99515', '99507', '99752', '99712', '99577',
            '99516', '99736', '99611', '99581', '99679', '99540', '99640', '99620', '99658', '99672', '99724', '99554',
            '99610', '99605', '99749', '99669', '99587', '99552', '99731', '99568', '99647', '99714', '99551', '99572',
            '99650', '99621', '99737', '99606', '99639', '99721', '99631', '99762', '99559', '99680', '99637', '99545',
            '99634', '99556', '99641', '99761', '99632', '99609', '99636', '99580', '99664', '99740', '99781', '99625',
            '99730', '99693', '99603', '99666', '99772', '99778', '99555', '99686', '99663', '99662', '99588', '99750',
            '99563', '99655', '99785', '99633', '99733', '99576', '99776', '99578', '99613', '99604', '99569', '99670',
            '99780', '99628', '99788', '99678', '99783', '99622', '99586', '99681', '99677', '99573', '99561', '99690',
            '99722', '99566', '99614', '99732', '99651', '99589', '99789', '99579', '99738', '99764', '99766', '99550',
            '99644', '99649', '99723', '99574', '99619', '99615', '99608', '99759', '99734', '99791', '99624', '99630',
            '99697', '99769', '99643', '99782', '99549']
AL_99638 = ['99638', '99692', '99685', '99553', '99583', '99547', '99591', '99612', '99660', '99661', '99571', '99546',
            '99648', '99548', '99565', '99564', '99549', '99589', '99651']



def make():
    all_zips = pd.read_excel('uszipcode.xlsx')

    # Add radius
    all_zips['radius'] = all_zips['zipcode'].apply(get_radius)

    # URL = f'{base_url}&rd=10&zc={str(zip_code)}' #&stkTypId=28881&

    # Add URL_end
    for i in all_zips.index:
        try:
            z = str(all_zips.loc[i,'zipcode'])
            if len(z) == 4:
                z = '0'+z
            r = math.ceil(all_zips.loc[i,'radius'])
            URL_end = f'rd={r}&zc={z}'
            all_zips.loc[i, 'URL_end'] = URL_end
            print(z)
        except Exception as e:
            print(z,e)

    all_zips.to_excel('uszipcode2.xlsx', index=0)


all_zips = pd.read_excel('uszipcode2.xlsx')
print(f'data read')
all_zips_dict={}

def evaluate_zips(M):
    p = .25 #percentage shrink in radius to avoid overlap
    zips_df = all_zips.copy()
    zips_df['Listing_Count'] = ''

    N = len(zips_df)
    ll = int(N/10)*(M-1)
    uu = min(N, int(N/10)*(M))

    print(f'evaluating zips_df.index[{ll}:{uu}]')
    # Add Evaluate All URLs
    session = False
    for i in zips_df.index[ll:uu]:
        try:
            print(f'{M}-{i}')
            if zips_df.loc[i,'Listing_Count']!='':
                continue

            z = str(zips_df.loc[i,'zipcode'])
            if len(z) == 4:
                z = '0'+z

            r = zips_df.loc[i,'radius']
            r = max(1, round(r*(1-p)))

            URL_end = zips_df.loc[i, 'URL_end']
            URL = f'{base_url}&{URL_end}'
            Listing_Count, Max_Pages, Pages, session = Evaluate(URL, session)
            zips_df.loc[i, 'Listing_Count'],zips_df.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            print(f'{M}-{(i-ll)}  Zip:{z}  #:{Listing_Count}  PP:{Max_Pages} i:{i}')
        except Exception as e:
            print(i,e)

        if (i-ll)%100==0:
            zips_df[ll:uu].to_excel(f'uszipcode3_chunk{M}.xlsx')
    zips_df[ll:uu].to_excel(f'uszipcode3_chunk{M}.xlsx')



def breakdown_zips(M):
    zips_df = pd.read_excel(f'uszipcode4.xlsx')

    zips_df = zips_df[zips_df['Listing_Count']>5000]
    zips_df.loc[zips_df['zipcode']==33196, ['radius','URL_end']] = [8, 'rd=8&zc=33196'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==90274, ['radius','URL_end']] = [8, 'rd=8&zc=90274'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==98320, ['radius','URL_end']] = [10, 'rd=10&zc=98320'] #zipcode raidus error

    zips_df.loc[zips_df['zipcode']==33327, ['radius','URL_end']] = [5, 'rd=5&zc=33327'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==85207, ['radius','URL_end']] = [6, 'rd=6&zc=85207'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==33428, ['radius','URL_end']] = [5, 'rd=5&zc=33428'] #zipcode raidus error


    zips_df.loc[zips_df['zipcode']==93030, ['radius','URL_end']] = [8, 'rd=8&zc=93030'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==98252, ['radius','URL_end']] = [10, 'rd=10&zc=98252'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==92311, ['radius','URL_end']] = [15, 'rd=5&zc=92311'] #zipcode raidus error
    zips_df.loc[zips_df['zipcode']==91042, ['radius','URL_end']] = [10, 'rd=10&zc=91042'] #zipcode raidus error
    zips_df.head()

    #Break down by color
    color_dict = {'White':['27134'],'Silver':['27133'], 'Gray':['27127'], 'Blue':['27124'], 'Black':['27123'],
                  'Other': ['27122','27125', '27126', '27128', '27129', '29637', '27131', '27132', '27135']}

    color_extensions = ['&clrId='+'%2C'.join(color) for color in color_dict.values()]

    zips_df_modified = pd.DataFrame(columns=zips_df.columns)
    for i in range(len(color_extensions)):
        print(color_extensions[i])
        # add each color to the URL and append.
        temp = zips_df.copy()
        temp['URL_end'] = temp['URL_end'] + color_extensions[i]
        zips_df_modified = zips_df_modified.append(temp)

    zips_df_modified.reset_index(drop=True, inplace=True)
    zips_df_modified.URL_end
    zips_df_modified['Listing_Count'] = ''
    zips_df_modified['Max_Pages'] = ''

    # Later also break down based on inventory type.
    zips_df_modified.to_excel(f'uszipcode_oversized.xlsx', index=False)


def evaluate_oversized_zips(M):
    zips_df = pd.read_excel(f'uszipcode_oversized.xlsx')

    N = len(zips_df)
    ll = int(N/10)*(M-1)
    uu = min(N, int(N/10)*(M))

    print(f'evaluating zips_df.index[{ll}:{uu}]')
    # Add Evaluate All URLs

    for i in zips_df.index[ll:uu]:
        try:
            print(f'{M}-{i}')
            if zips_df.loc[i,'Listing_Count']!='' and pd.notna(zips_df.loc[i,'Listing_Count']):
                continue

            z = str(zips_df.loc[i,'zipcode'])
            if len(z) == 4:
                z = '0'+z

            URL_end = zips_df.loc[i, 'URL_end']
            URL = f'{base_url}&{URL_end}'
            Listing_Count, Max_Pages, Pages = Evaluate(URL)
            zips_df.loc[i, 'Listing_Count'],zips_df.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            print(f'{M}-{(i-ll)}  Zip:{z}  #:{Listing_Count}  PP:{Max_Pages} i:{i}')
        except Exception as e:
            print(i,e)
        if (i-ll)%100==0:
            zips_df[ll:uu].to_excel(f'uszipcode_oversized_chunk{M}.xlsx')
    zips_df[ll:uu].to_excel(f'uszipcode_oversized_chunk{M}.xlsx')



def evaluate_zips_inparallel():
    pool = Pool()
    pool.map(evaluate_zips,range(1,11))  #10 parallel threads
    pool.close()
    pool.join()

    z = pd.read_excel(f'uszipcode3_chunk1.xlsx')
    for M in range(2,11):
        z = z.append(pd.read_excel(f'uszipcode3_chunk{M}.xlsx'))
    z_df.to_csv('uszipcode4.csv', index=False)


def fix_oversized_zips():
    pool = Pool()
    pool.map(evaluate_oversized_zips, range(1, 11))  # 10 parallel threads
    pool.close()
    pool.join()

    z = pd.read_excel(f'uszipcode_oversized_chunk1.xlsx')
    for M in range(2,11):
        z = z.append(pd.read_excel(f'uszipcode_oversized_chunk{M}.xlsx'))
    z = z[z['Listing_Count']!=0]

    z.to_csv('uszipcode_oversized_complete.csv', index=False)
    z = pd.read_csv('./hist/uszipcode_oversized_complete.csv')
    z[['URL_end','Listing_Count']]
    z['Max_Pages'] =
    z = z[pd.notna(z['Listing_Count'])]

    zips_df.URL_end

    ## Now replacing the broken zips with the oversized ones.
    zips_df = pd.read_excel(f'./hist/uszipcode4.xlsx')
    zips_df = zips_df.drop_duplicates(['zipcode'])
    zips_df = zips_df[zips_df['Listing_Count'] < 5000]
    zips_df = zips_df.append(z)
    zips_df = zips_df[pd.notna(zips_df.Listing_Count)]
    cols = [c for c in zips_df.columns if 'Unnamed' not in c and 'Listing_Count2' not in c]
    zips_df = zips_df[cols]
    zips_df['URL_end'] = '&' + zips_df['URL_end']
    zips_df['Max_Pages'] = (zips_df['Listing_Count'] / 100).apply(math.ceil)
    zips_df = pd.read_csv('scrape_guide.csv')

    random.seed(100)
    zips_df['random'] = np.random.random(len(zips_df))
    zips_df = zips_df.sort_values('random')

    zips_df.to_csv('scrape_guide.csv', index=False)
    return zips_df


def Shuffle_Pages(max_pages):
    Shuffled_Pages = list(range(1, max_pages+1))
    random.shuffle(Shuffled_Pages)
    #print(f'Page order: {Shuffled_Pages}')
    return Shuffled_Pages

def Add_Pages(Guide):
    Guide = pd.read_csv('scrape_guide.csv')
    '''create one new line for each page'''
    Guide_With_Pages = pd.DataFrame()
    for i in Guide.index:
        Max_Pages = Guide.loc[i,'Max_Pages']
        Shuffled_Pages = Shuffle_Pages(Max_Pages)
        print(f'{i}- Pages: {Max_Pages}')
        URL_End =  Guide.loc[i,'URL_end']

        for p in Shuffled_Pages:
            new_row = Guide[i:i+1].copy()
            new_row.loc[i, 'URL_end'] = f'&page={p}{URL_End}' # Add Page number
            new_row.loc[i, 'Page'] = p
            Guide_With_Pages = Guide_With_Pages.append(new_row)

    Guide_With_Pages = Guide_With_Pages[['zipcode','URL_end','Page']]
    Guide_With_Pages.to_csv('Guide_With_Pages.csv', index=0)
    return Guide_With_Pages


def replace_radius(url):
    p = .25 #percent reduction in radius to avoid overlap
    r = re.search("rd=([\d]+)", url)[1]
    r_new = round(max(int(r)*(1-p) , 1))
    return url.replace(f'rd={r}',f'rd={r_new}')


def Update_Guide(M):
    Guide = pd.read_csv('./guide/scrape_guide.csv')
    Guide['URL_end'] = Guide['URL_end'].apply(replace_radius)

    Guide['Listing_Count_old'] = Guide['Listing_Count']
    Guide['Listing_Count'] = ''

    M = 1

    N = len(Guide)
    ll = int(N/10)*(M-1)
    uu = min(N, int(N/10)*(M))

    print(f'evaluating Guide.index[{ll}:{uu}]')
    # Add Evaluate All URLs
    session = False
    for i in Guide.index[ll:uu]:
        try:
            print(f'{M}-{i}')
            if Guide.loc[i,'Listing_Count']!='':
                continue

            z = str(Guide.loc[i,'zipcode'])
            if len(z) == 4:
                z = '0'+z

            r = Guide.loc[i,'radius']

            URL_end = Guide.loc[i, 'URL_end']
            URL = f'{base_url}&{URL_end}'
            Listing_Count, Max_Pages, Pages, session = Evaluate(URL, session)
            Guide.loc[i, 'Listing_Count'],Guide.loc[i, 'Max_Pages'] = Listing_Count, Max_Pages
            print(f'{M}-{(i-ll)}  Zip:{z}  #:{Listing_Count}  PP:{Max_Pages} i:{i}')
        except Exception as e:
            print(i,e)

        if (i-ll)%100==0:
            Guide[ll:uu].to_csv(f'Guide_Updated_chunk{M}.xlsx')
    Guide[ll:uu].to_csv(f'Guide_Updated_chunk{M}.xlsx')




if __name__ == '__main__':
    evaluate_zips_inparallel()
    fix_oversized_zips()
    Add_Pages()


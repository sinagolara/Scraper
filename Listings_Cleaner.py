
import pandas as pd
from pandas.errors import EmptyDataError
from glob import glob
import os, sys
import shutil
from json_flatten import flatten
import zipfile
from pathlib import Path
from cleaning.file_management import *
from cleaning.address_management import *
from cleaning.time_management import *
import json
import re

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 180)



def bool_to_int(bool_string):
    if str(bool_string).lower() == 'true' or str(bool_string)=='1':
        int_string = 1
    else:
        int_string = 0
    return int_string

def extract_date(s):
    m = re.search(r'20\d\d-\d\d-\d\d', s)
    if m:
        return m[0]
    else:
        return None


# listings = inv.copy()
def old_clean_listings(listings):
    if len(listings) == 0:
        return pd.DataFrame()

    col_rename_dict = {'@context':'context',
                    '@type':'type',
                    'vehicleIdentificationNumber':'vin',
                    'name':'name',
                    'image':'image',
                    'brand.@type':'brand.type',
                    'brand.name':'brand.name',
                    'offers.@type':'offers.@type',
                    'offers.priceCurrency':'priceCurrency',
                    'offers.price$int':'price',
                    'offers.availability':'availability',
                    'offers.seller.@type':'seller.type',
                    'offers.seller.name':'seller.name',
                    'offers.seller.telephone':'seller.telephone',
                    'offers.seller.address.@type':'seller.address.type',
                    'offers.seller.address.addressLocality':'seller.city',
                    'offers.seller.address.addressRegion':'seller.state',
                    'offers.seller.address.streetAddress':'seller.streetAddress',
                    'offers.seller.aggregateRating.@type':'seller.rating.@type',
                    'offers.seller.aggregateRating.ratingValue$float':'seller.rating',
                    'offers.seller.aggregateRating.reviewCount':'seller.reviewCount',
                    'offers.itemCondition':'itemCondition',
                    'url':'url',
                    'color':'color',
                    'offers.seller.aggregateRating.ratingValue$int':'seller.rating.value$int',
                    'offers.price$none':'price$none'}
    listings = listings.rename(col_rename_dict, axis=1)
    listings = listings.reset_index(drop=True)
    if 'seller.rating.value$int' in listings.columns:
        listings.loc[pd.isna(listings['seller.rating']), 'seller.rating'] = listings['seller.rating.value$int']

    cols_wanted = ['vin', 'name', 'brand.type', 'brand.name',
                         'availability', 'price', 'price$none',
                         'seller.type', 'seller.name', 'seller.telephone',
                         'seller.city', 'seller.state', 'seller.streetAddress',
                         'seller.rating.@type', 'seller.rating', 'seller.reviewCount',
                         'itemCondition', 'color', 'url',
                         'seller.rating.value$int', 'payment_list.0', 'payment_list.1', 'deal',
                         'hot_badge', 'savings_badge', 'carvana_badge', 'local_home_delivery', 'national_home_delivery',
                         'virtual_appointments', 'download_time']
    cols_intersection = set(cols_wanted).intersection(listings.columns)

    listings = listings[cols_intersection]
    listings = listings.drop_duplicates([c for c in listings.columns if c not in ['search_id', 'zip']])

    if 'availability' in listings.columns:
        listings['availability'] = listings['availability'].apply(lambda s: str(s).replace('http://schema.org/','').replace('InStock',''))
    listings['brand.type'] = listings['brand.type'].apply(lambda s: str(s).replace('Organization', 'O'))
    listings = listings[~listings['brand.type'].isin(['Person', 'Prsn'])]

    listings['seller.type'] = listings['seller.type'].apply(lambda s: str(s).replace('Organization','').replace('Person','Prsn'))
    listings['seller.rating.@type'] = listings['seller.rating.@type'].apply(lambda s: str(s).replace('AggregateRating','Agg'))
    listings['itemCondition'] = listings['itemCondition'].apply(lambda s: str(s).replace('http://schema.org/UsedCondition','U').replace('http://schema.org/NewCondition','N'))
    listings['url'] = listings['url'].apply(lambda s: str(s).replace('https://www.cars.com/vehicledetail/detail',''))
    listings['url base: https://www.cars.com/vehicledetail/detail'] = ''
    listings['deal'] = listings['deal'].apply(lambda s: str(s).replace(' Deal','').replace(' Price','').replace('nan',''))

    # standardize values
    listings['local_home_delivery'] = listings['local_home_delivery'].apply(bool_to_int)
    listings['national_home_delivery'] = listings['national_home_delivery'].apply(bool_to_int)
    listings['virtual_appointments'] = listings['virtual_appointments'].apply(bool_to_int)
    if 'hot_badge' in listings.columns:
        listings['hot_badge'] = listings['hot_badge'].apply(bool_to_int)
    if 'savings_badge' in listings.columns:
        listings['savings_badge'] = listings['savings_badge'].apply(bool_to_int)
    if 'carvana_badge' in listings.columns:
        listings['carvana_badge'] = listings['carvana_badge'].apply(bool_to_int)

    return listings

def old_clean_col_names(listings):
    if len(listings) == 0:
        return pd.DataFrame()

    col_rename_dict = {'@context':'context',
                    '@type':'type',
                    'vehicleIdentificationNumber':'vin',
                    'name':'name',
                    'image':'image',
                    'brand.@type':'brand.type',
                    'brand.name':'brand.name',
                    'offers.@type':'offers.@type',
                    'offers.priceCurrency':'priceCurrency',
                    'offers.price$int':'price',
                    'offers.availability':'availability',
                    'offers.seller.@type':'seller.type',
                    'offers.seller.name':'seller.name',
                    'offers.seller.telephone':'seller.telephone',
                    'offers.seller.address.@type':'seller.address.type',
                    'offers.seller.address.addressLocality':'seller.city',
                    'offers.seller.address.addressRegion':'seller.state',
                    'offers.seller.address.streetAddress':'seller.streetAddress',
                    'offers.seller.aggregateRating.@type':'seller.rating.@type',
                    'offers.seller.aggregateRating.ratingValue$float':'seller.rating',
                    'offers.seller.aggregateRating.reviewCount':'seller.reviewCount',
                    'offers.itemCondition':'itemCondition',
                    'url':'url',
                    'color':'color',
                    'offers.seller.aggregateRating.ratingValue$int':'seller.rating.value$int',
                    'offers.price$none':'price$none'}
    listings = listings.rename(col_rename_dict, axis=1)
    listings = listings.reset_index(drop=True)
    listings.loc[pd.isna(listings['seller.rating']), 'seller.rating'] = listings['seller.rating.value$int']

    cols_wanted = ['vin', 'name', 'brand.type', 'brand.name',
                         'availability', 'price', 'price$none',
                         'seller.type', 'seller.name', 'seller.telephone',
                         'seller.city', 'seller.state', 'seller.streetAddress',
                         'seller.rating', 'seller.reviewCount',
                         'itemCondition', 'color', 'url',
                         'seller.rating.value$int', 'payment_list.0', 'payment_list.1', 'deal',
                         'hot_badge', 'savings_badge', 'carvana_badge', 'local_home_delivery', 'national_home_delivery',
                         'virtual_appointments', 'download_time']
    cols_intersection = set(cols_wanted).intersection(listings.columns)

    listings = listings[cols_intersection]
    return listings

# listings = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Second Batch/clean_listings_2021-03-18.csv')
# new = new_clean_listings(listings)
# ff = old_clean_col_names(listings)
# zz = new_clean_listings(ff)
# ff.columns
# zz.columns
# ff['savings_badge']
# ff['deal']
# zz['deal']
# ff['carvana_badge'].unique()



#
# x = pattern_merge(pattern='./listings/listings_2020-08-14.csv')
# y = x.drop_duplicates([c for c in x.columns if c not in ['search_id', 'zip']])
# y.to_csv('./listings/listings_2020-08-14.csv', index=False)
# len(y)
pattern_zip('./listings/clean_listings_*.csv', move_to_folder='./recycle_bin')


def clean_all_listings():
    print(f'cleaning raw listings')

    # sorted file paths
    paths = glob(f'./listings/listings_202?-??-??*.zip')
    print(f'{len(paths)} paths found.')
    paths.sort()

    # read
    for i in range(len(paths)):
        try:
            p = paths[i]
            ## skip if already done
            date = extract_date(p)
            output_path = f'./listings/clean_listings_{date}.zip'
            if os.path.exists(output_path):
                print(f'{output_path} exists')
                continue

            csv, csv_name = read_zipped_csv(p)
            ## fix if old naming style:
            if 'listings ' in csv_name:
                csv_name = csv_name.replace('listings ', 'listings_2020-')
            cleaned = old_clean_listings(csv)
            # date = extract_date(csv_name)
            # output_path = f'./listings/clean_listings_{date}.zip'
            # cleaned.to_csv(output_path, index=False)
            df_to_zip(cleaned, output_path)
            size = os.path.getsize(output_path)//1024**2
            print(f'{i}-{Path(p).name} date {date} saved @ {output_path} shape={cleaned.shape} size={size}MB')

        except Exception as e:
            print(e)

    return

# listings = all_listings.copy()

def new_clean_listings(listings):
    if len(listings) == 0:
        return pd.DataFrame()

    col_rename_dict = {'@context':'context',
                    'vehicleIdentificationNumber':'vin',
                    'name':'car_name',
                    'image':'image',
                    '@type':'item_type',
                    'brand.type':'brand_type',
                    'brand.@type':'brand_type',
                    'image.@type':'image_type',
                    'brand.name':'brand',
                    'make':'brand',
                    'sponsored?':'sponsored',
                    'image.contentUrl':'image_url',
                    'stock_type':'itemCondition',
                    'url':'URL',
                    'link':'url',
                    'price$none':'price',
                    'seller.name':'dealer_name',
                    'seller.telephone':'telephone',
                    'seller.rating': 'dealer_rating',
                    'seller.reviewCount':'dealer_reviews',
                    'local_home_delivery': 'home_delivery'
                       }

    listings = listings.rename(col_rename_dict, axis=1)
    listings = listings.reset_index(drop=True)

    cols_to_drop = ['context', 'item_type', 'offers', 'listing_id',
     'results_page_number', 'sponsored', 'stock_type', 'image_type', 'image_url']

    cols_wanted = [c for c in listings.columns if c not in cols_to_drop]

    listings = listings[cols_wanted]

    must_have_col = ['brand_type', 'itemCondition', 'deal', 'url', 'home_delivery', 'virtual_appointments',
                     'hot_badge', 'online_seller']
    for c in must_have_col:
        if c not in listings.columns:
            listings[c] = np.nan

    listings['brand_type'] = listings['brand_type'].replace('Organization', 'O')

    # listings['seller.type'] = listings['seller.type'].apply(lambda s: str(s).replace('Organization','').replace('Person','Prsn'))
    # listings['seller.rating.@type'] = listings['seller.rating.@type'].apply(lambda s: str(s).replace('AggregateRating','Agg'))
    listings['itemCondition'] = listings['itemCondition'].apply(lambda s: str(s).
                                                                replace('http://schema.org/UsedCondition', 'U').
                                                                replace('http://schema.org/NewCondition', 'N').
                                                                replace('used', 'U').replace('new', 'N'))

    listings['url'] = listings['url'].astype(str).apply(lambda s: s.replace('vehicledetail/', ''))
    listings['url base: https://www.cars.com/vehicledetail/'] = ''
    listings['deal'] = listings['deal'].apply(lambda s: str(s).replace(' Deal','').replace(' Price','').replace('nan','').strip())
    listings['online_seller'] = listings['online_seller'].replace('nan', '').replace('Online seller', '1').apply(bool_to_int)

    # standardize values
    listings['home_delivery'] = listings['home_delivery'].apply(bool_to_int)
    listings['virtual_appointments'] = listings['virtual_appointments'].apply(bool_to_int)
    listings['hot_badge'] = listings['hot_badge'].apply(bool_to_int)
    listings['cpo_indicator'] = listings['cpo_indicator'].apply(bool_to_int)

    return listings

def filter_VINs(skip_if_exists = True):
    print('START filter_VINs')

    ###### ONly keep VINS ######
    print('unzipping & converting clean_listings => VINs')
    # sorted file paths
    paths = glob(f'./listings/clean_listings_202?-??-??.zip')
    paths.sort()
    stems = [Path(p).stem for p in paths]
    dates = [s.replace('clean_listings_', '') for s in stems]

    for i in range(0, len(paths)):
        try:
            p = paths[i]
            date = dates[i]
            output_path = f'{Path(p).parent}/VINs_{date}.csv'
            if skip_if_exists: ## if it exists do not try to recreate
                if os.path.exists(output_path):
                    print(f'\t {output_path} already exists.. skipping to the next one..')
                    continue
            VINs = dict()
            csv = read_zipped_csv(p)
            vins = csv[0]['vin']
            #VINs[date] = csv[0]['vin']
            vins.to_csv(output_path, index=False)
            print(f'{i}- date {date} saved @ {output_path}')
        except Exception as e:
            print(e)
    print('FINISH filter_VINs')


def find_sold_vins(skip_if_exists=True):
    print('START filter_VINs')

    ## First check if this functions output, sold.json, already exists
    sold_json_path = "./sales/sold.json"
    if skip_if_exists & os.path.exists(f'{sold_json_path}'):  # first check existing, to avoid redoing
        print(f'{sold_json_path} already exists. reading it in..')
        with open(f"{sold_json_path}", "r") as f:
            sold_json = json.loads(f.read())
            return sold_json

    ###### Read VINs by Date ######
    print('converting VINs => sold.json')

    # sorted file paths
    paths = glob(f'./listings/VINs_202?-??-??.csv')
    paths.sort()
    dates = [Path(p).stem.replace('VINs_', '') for p in paths]

    # read
    print('aggregating all sold VINs from all dates')

    all_dates_path = './listings/VINs_all_dates.json'
    if skip_if_exists & os.path.exists(f'{all_dates_path}'): # first check existing, to avoid redoing
        print(f'{all_dates_path} already exists. reading it in..')
        with open(f"{all_dates_path}", "r") as f:
            all_dates = json.loads(f.read())

    else:
        all_dates = dict()
        for i in range(len(paths)):
            try:
                p = paths[i]
                date = dates[i]
                all_dates[date] = pd.read_csv(p)['vin'].tolist()
                print(f'{i}- date {date} read')
            except Exception as e:
                print(e)
                raise
        ## save all_dates.json
        Json = json.dumps(all_dates)
        with open(f"{all_dates_path}", "w") as f:
            f.write(Json)

        print(f'{all_dates_path} saved')

    ###### Compute Sales ######
    ## find what is sold: compute today's difference from future days.
    all_sales = all_dates.copy()
    dates = list(all_sales.keys())

    for i in range(len(dates)):
        try:
            today_vins = set(all_sales[dates[i]])
            remaining_days = len(dates) - i - 1
            how_far_out = max(min(remaining_days, 8), 1) #at least 1 days and at most 8 days out
            future_vins = set()
            for j in range(1, how_far_out+1):
                future_vins = future_vins | set(all_sales[dates[i+j]])
                print(f'\tadded {dates[i+j]} with {len(set(all_sales[dates[i+j]]))} VINs. '
                      f'Now {len(future_vins)} VINs found looking {j} day(s) into the future ')

            today_sales = today_vins - future_vins
            print (f'\ttoday_sales {len(today_sales)} = today_vins {len(today_vins)} EXCLUDING future_vins {len(today_vins)}')
            all_sales[dates[i]] = list(today_sales)
            print(f'\t{dates[i]} sales recorded. NOW, all_sales has {len(all_sales)} rows')


            print(f'{i}-{dates[i]}\t Inv: {len(today_vins)}\t futureVINS: {len(future_vins)}\t Sales:{len(all_sales[dates[i]])}\t [{how_far_out} days out were considered]')
            all_sales[dates[i]]

        except Exception as e:
            print(e)

    sold_json = json.dumps(all_sales)
    with open(f"./sales/sold.json", "w") as f:
        f.write(sold_json)


    print('FINISHED: saved @ ./sales/sold.json ')
    return

# inv_path = './listings/clean_listings_2020-12-26.zip'
# sold_json.keys()
# VINs_2020-12-28

def inventory_to_sales(inv_path, sold_json):
    print('START inventory_to_sales: extracting only sold VINS from inv')

    if inv_path.endswith('.zip'):
        csv, fname = read_zipped_csv(inv_path)
    else:
        csv = pd.read_csv(inv_path)
        _, fname, stem, suffix = parse_path(inv_path)
    inv = csv.drop_duplicates()
    # print('cleaning again because the cleaning functions has since been updated')
    # inv = old_clean_listings(inv)

    date = fname.replace('clean_listings_', '').replace('.csv', '')
    if date in sold_json:
        sold = sold_json[date]
    elif f'VINs_{date}' in sold_json:
        sold = sold_json[f'VINs_{date}']
    else:
        print(f'{date} or VINs_{date} not found in json keys')
        raise Exception
    sales = inv.loc[inv['vin'].isin(sold)].copy()

    sales['date'] = date
    print('FINISHED inventory_to_sales')

    return sales, date


def gen_all_sales(sold_json):
    ###### ONly keep SOLD ######
    print('START gen_all_sales')

    # zip_path = '/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Second Batch/clean_listings_2020-08-24.zip'
    # sorted file paths
    paths = glob(f'./listings/clean_listings_202?-??-??.zip')
    # paths = glob(f'./listings/clean_listings_2021-01-19.zip')
    paths.sort()
    stems = [Path(p).stem for p in paths]
    dates = [s.replace('clean_listings_', '') for s in stems]
    min_date = min(dates, key=lambda d: string_to_datetime(d))
    max_date = max(dates, key=lambda d: string_to_datetime(d))
    print(f'Sales files found from {min_date} to {max_date}')

    # i=0
    for i in range(len(paths)):
        try:
            p = paths[i]
            print(f'{i}- generating sales from \t {p} ...')
            sales, date = inventory_to_sales(inv_path=p, sold_json=sold_json)
            output_path = f'./sales/sales_{date}.csv'
            sales.to_csv(output_path, index=False)
            print(f'\t\tdate {date} sales saved @ {output_path}')
        except Exception as e:
            print('error:', e)
    # aggregate all sales
    all_sales = pattern_merge('./sales/sales_202?-??-??.csv', drop_duplicates=True, move_to_folder='./hist')
    df_to_zip(all_sales, zip_path=f'./sales/allsales_{min_date}_to_{max_date}.zip')

    return

# all_listings_clean = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/2- AutoDataLoop/listings/clean_listings_2021-06-28.csv')

def gen_listings_report(all_listings_clean, warning_path=None):
    R = {}
    if len(all_listings_clean) == 0:
        print('all_listings_clean is empty. cannot generate report')
        R['empty'] = 'all_listings_clean is empty'
    else:
        R['n_rows'] = len(all_listings_clean)
        R['vins'] = all_listings_clean['vin'].nunique()

        R['dealers'] = all_listings_clean['dealer_name'].nunique()
        # R['cities'] = all_listings_clean['seller.city'].nunique()
        # R['states'] = all_listings_clean['seller.state'].nunique()
        cols = all_listings_clean.columns
        R['hot_badges'] = R['virtual_appointments'] = R['download_time'] = R['home_deliveries'] \
            = R['dealers_w_delivery'] = ''
        if 'hot_badge' in cols:
            R['hot_badges'] = all_listings_clean['hot_badge'].sum()
        if 'virtual_appointments' in cols:
            R['virtual_appointments'] = all_listings_clean['virtual_appointments'].sum()
            R['dealers_w_virtual'] = all_listings_clean.groupby(['dealer_name']).virtual_appointments.agg('max').sum()
        if 'download_time' in cols:
            R['download_time'] = all_listings_clean.loc[pd.notnull(all_listings_clean['download_time']), 'download_time'][0]
        if 'home_delivery' in cols:
            R['home_deliveries'] = all_listings_clean['home_delivery'].sum()
            R['dealers_w_delivery'] = all_listings_clean.groupby(['dealer_name']).home_delivery.agg('max').sum()
        if 'online_sellers' in cols:
            # R['online'] = all_listings_clean['online_seller'].sum()
            R['online_sellers'] = all_listings_clean.groupby(['dealer_name']).online_seller.agg('max').sum()

    # Access the warnings file and count
    if warning_path:
        if os.path.exists(warning_path):
            with open(warning_path, 'r') as f:
                F = f.readlines()
                R['num_no_match'] = len([x for x in F if 'no matches message:' in x])
                R['num_too_many'] = len([x for x in F if 'too many:' in x])
        else:
            R['warning'] = f'{warning_path} not found'

    report_df = pd.DataFrame(R, index=[1])
    return report_df

def gen_all_reports():
    paths = glob(f'./listings/clean_listings_2020-??-??.zip')
    paths.sort()
    all_reports = pd.DataFrame()
    for i in range(len(paths)):
        try:
            p = paths[i]
            listings, date = read_zipped_csv(zip_path=p)
            new_report = gen_listings_report(old_clean_listings(listings))
            new_report['date'] = date
            all_reports = all_reports.append(new_report)
            print(f'{i}- date {date} report done')
        except Exception as e:
            print('error:', e)
    all_reports.to_csv(f'./all_listings_report.csv', index = False)
    return


def CountFrequency(lst):
    count = {}
    for i in lst:
        count[i] = count.get(i, 0) + 1
    return list(count.values())

def HHI(lst):
    R = [l / sum(lst) for l in lst]  # ratio of each item
    hhi = sum([r ** 2 for r in R]) ** .5
    return hhi

def HHI_Cat(lst): ## HHI for categorical list
    return HHI(CountFrequency(lst))

def old_listings_to_daily_inv(inv, date):
    ##### Make Dealer Book ''' output: dealer-day-inventory '''

    # Read all sales datasets
    # all_sales = pattern_merge('./sales/sales_2020-??-??.csv', drop_duplicates=True)
    # all_sales = pd.read_csv('./sales/allsales.csv')
    # df_to_zip(all_sales, zip_path='./sales/allsales.zip')

    # sales = read_zipped_csv(zip_path='./sales/allsales.zip')[0]
    # print(f'all columns: {sales.columns}')
    ### Only select dealer info
    dealer_cols = ['seller.city', 'seller.telephone', 'seller.streetAddress',
           'local_home_delivery', 'seller.reviewCount',
           'virtual_appointments', 'seller.name',
           'national_home_delivery',
           'seller.state', 'seller.rating.value$int',
           'seller.rating', 'price', 'brand.name','name','itemCondition', 'deal']

    # inv = inv.sort_values('seller.name')
    dealers = inv[dealer_cols].sort_values('seller.name').copy()

    # df_to_zip(dealers, zip_path='./sales/dealers.zip')
    # dealers2 = read_zipped_csv(zip_path='./sales/dealers.zip')[0]
    # dealers = dealers2 [0:100000]
    dealers['date'] = pd.to_datetime(date, infer_datetime_format=True)
    dealers['dealer_id'] = dealers.groupby(['seller.name', 'seller.city']).ngroup()

    print('transformations')

    # standardize values
    dealers.local_home_delivery.unique()

    # active_local_del: Find the maximum (whether service is active) per day per dealer
    dealers['active_local_del'] = dealers.groupby(['dealer_id', 'date']).local_home_delivery.transform('max')
    dealers['local_del_ratio'] = dealers.groupby(['dealer_id', 'date']).local_home_delivery.transform('mean')

    # national delivery:
    dealers['active_national_del'] = dealers.groupby(['dealer_id', 'date']).national_home_delivery.transform('max')
    dealers['national_del_ratio'] = dealers.groupby(['dealer_id', 'date']).national_home_delivery.transform('mean')

    # virt appt:
    dealers['active_virt_apt'] = dealers.groupby(['dealer_id', 'date']).virtual_appointments.transform('max')

    print('\t deals')
    # deals
    # dealers['deal'].unique()
    dealers['num_good_deal'] = dealers.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Good').sum())
    dealers['num_great_deal'] = dealers.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Great').sum())
    dealers['num_fair_deal'] = dealers.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Fair').sum())

    ## num cars/brands/new-used
    print('\t num cars/brands/new-used')
    dealers['num_cars'] = dealers.groupby(['dealer_id', 'date']).name.transform(lambda x: x.count())
    dealers['num_unique_brands'] = dealers.groupby(['dealer_id', 'date'])['brand.name'].transform(lambda x: x.nunique())
    dealers['num_unique_models'] = dealers.groupby(['dealer_id', 'date'])['name'].transform(lambda x: x.nunique())
    dealers['num_used'] = dealers.groupby(['dealer_id', 'date'])['itemCondition'].transform(lambda x: (x == 'U').sum())
    dealers['num_new'] = dealers.groupby(['dealer_id', 'date'])['itemCondition'].transform(lambda x: (x == 'N').sum())

    print('\t HHI')
    # Herfindahl Index for model/brand variety
    # dealers.loc[dealers.dealer_id == 8, ['dealer_id','seller.name', 'brand.name', 'name', 'brands_HHI', 'models_HHI',
    #                                      'usedBrandHHI']]
    # 1/6*(5**2+1)**.5
    # for i in dealers.loc[(dealers.dealer_id == 8) & (dealers.itemCondition=='U'), ['brand.name', 'itemCondition']]:
    #     print(i)

    dealers.loc[dealers['brand.name'].notna(), 'brands_HHI'] = dealers[dealers['brand.name'].notna()].groupby(['dealer_id', 'date'])['brand.name'].transform(HHI_Cat)
    dealers.loc[dealers['name'].notna(), 'models_HHI'] = dealers[dealers['name'].notna()].groupby(['dealer_id', 'date']).name.transform(HHI_Cat)
    dealers.loc[(dealers.itemCondition=='N') & (dealers['name'].notna()), 'newModelsHHI'] = \
        dealers[(dealers.itemCondition=='N') & (dealers['name'].notna())].groupby(['dealer_id', 'date']).name.transform(HHI_Cat)

    dealers.loc[(dealers.itemCondition=='U') & (dealers['name'].notna()), 'usedModelsHHI'] = \
        dealers[(dealers.itemCondition=='U') & (dealers['name'].notna())].groupby(['dealer_id', 'date']).name.transform(HHI_Cat)

    # fill NAs with max:
    for c in [cc for cc in dealers.columns if 'HHI' in cc]:
        dealers[c] = dealers.groupby(['dealer_id', 'date'])[c].transform(max)


    # dealers = dealers.dealer_id == 8

    print('\t find unique brands per dealers-day and merge')
    # find unique brands per dealers-day and merge
    unique_brands = dealers.groupby(['dealer_id', 'date'], as_index=False)['brand.name'].agg(lambda x: list(set(x)))
    dealers = pd.merge(dealers, unique_brands, on=['dealer_id', 'date'], how='left', suffixes=['', 's'])
    top_brand = dealers.groupby(['dealer_id', 'date'], as_index=False)['brand.name'].agg(lambda x: x.value_counts().index[0] if any([pd.notna(y) for y in x]) else '')

    dealers = pd.merge(dealers, top_brand, on=['dealer_id', 'date'], how='left', suffixes=['', '_top_brand'])
    dealers = dealers.rename({'brand.name_top_brand': 'top_brand'}, axis=1)

    # price
    print('\t price')
    dealers['price_avg'] = dealers.groupby(['dealer_id', 'date'])['price'].transform('mean')
    dealers['price_std'] = dealers.groupby(['dealer_id', 'date'])['price'].transform('std')

    # Collapse
    print('collapsing')
    dealers = dealers.drop(['price', 'brand.name', 'name','itemCondition', 'deal', 'local_home_delivery',
                            'national_home_delivery', 'virtual_appointments', 'price', 'name'], axis=1)

    dealer_day_inv = dealers.drop_duplicates(['dealer_id', 'date'])
    dealer_day_inv['dealer_city'] = dealer_day_inv['seller.name'] + dealer_day_inv['seller.city']

    return dealer_day_inv

def new_listings_to_daily_inv(inv, date):
    ##### Make Dealer Book ''' output: dealer-day-inventory '''
    ### Only select dealer info
    # dealer_cols = ['seller.city', 'seller.telephone', 'seller.streetAddress',
    #        'local_home_delivery', 'seller.reviewCount',
    #        'virtual_appointments', 'seller.name',
    #        'national_home_delivery',
    #        'seller.state', 'seller.rating.value$int',
    #        'seller.rating', 'price', 'brand.name','name','itemCondition', 'deal']

    # df_to_zip(dealers, zip_path='./sales/dealers.zip')
    # dealers2 = read_zipped_csv(zip_path='./sales/dealers.zip')[0]
    # dealers = dealers2 [0:100000]

    inv['date'] = pd.to_datetime(date, infer_datetime_format=True)
    inv['dealer_id'] = inv.groupby(['dealer_name', 'customer_id']).ngroup()

    print('transformations')
    # standardize values
    inv.home_delivery.unique()
    inv.online_seller.unique()
    inv.itemCondition.unique()

    # active_local_del: Find the maximum (whether service is active) per day per dealer
    inv['active_del'] = inv.groupby(['dealer_id', 'date']).home_delivery.transform('max')
    inv['del_ratio'] = inv.groupby(['dealer_id', 'date']).home_delivery.transform('mean')

    # national delivery:
    # inv['active_national_del'] = inv.groupby(['dealer_id', 'date']).national_home_delivery.transform('max')
    # inv['national_del_ratio'] = inv.groupby(['dealer_id', 'date']).online_seller.transform('mean')

    # virt appt:
    inv['active_virt_apt'] = inv.groupby(['dealer_id', 'date']).virtual_appointments.transform('max')

    print('\t deals')
    # deals
    # dealers['deal'].unique()
    inv['num_good_deal'] = inv.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Good').sum())
    inv['num_great_deal'] = inv.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Great').sum())
    inv['num_fair_deal'] = inv.groupby(['dealer_id', 'date']).deal.transform(lambda x: (x == 'Fair').sum())

    ## num cars/brands/new-used
    print('\t num cars/brands/new-used')
    inv['num_cars'] = inv.groupby(['dealer_id', 'date']).vin.transform(lambda x: x.nunique())
    inv['num_unique_brands'] = inv.groupby(['dealer_id', 'date'])['brand'].transform(lambda x: x.nunique())
    inv['num_unique_models'] = inv.groupby(['dealer_id', 'date'])['car_name'].transform(lambda x: x.nunique())
    inv['num_used'] = inv.groupby(['dealer_id', 'date'])['itemCondition'].transform(lambda x: (x != 'N').sum())
    inv['num_new'] = inv.groupby(['dealer_id', 'date'])['itemCondition'].transform(lambda x: (x == 'N').sum())

    # Herfindahl Index for model/brand variety
    print('\t HHI')
    inv.loc[inv['brand'].notna(), 'brands_HHI'] = inv[inv['brand'].notna()].groupby(['dealer_id', 'date'])['brand'].transform(HHI_Cat)
    inv.loc[inv['car_name'].notna(), 'models_HHI'] = inv[inv['car_name'].notna()].groupby(['dealer_id', 'date']).car_name.transform(HHI_Cat)
    inv.loc[(inv.itemCondition=='N') & (inv['car_name'].notna()), 'newModelsHHI'] = inv[(inv.itemCondition=='N') & (inv['car_name'].notna())].groupby(['dealer_id', 'date']).car_name.transform(HHI_Cat)
    inv.loc[(inv.itemCondition=='U') & (inv['car_name'].notna()), 'usedModelsHHI'] = inv[(inv.itemCondition=='U') & (inv['car_name'].notna())].groupby(['dealer_id', 'date']).car_name.transform(HHI_Cat)
    # fill NAs with max:
    for c in [cc for cc in inv.columns if 'HHI' in cc]:
        inv[c] = inv.groupby(['dealer_id', 'date'])[c].transform(max)

    # find unique brands per dealers-day and merge
    print('\t find unique brands per dealer-day and merge')
    unique_brands = inv.groupby(['dealer_id', 'date'], as_index=False)['brand'].agg(lambda x: list(set(x)))
    inv = pd.merge(inv, unique_brands, on=['dealer_id', 'date'], how='left', suffixes=['', 's'])

    top_brand = inv.groupby(['dealer_id', 'date'], as_index=False)['brand'].agg(lambda x: x.value_counts().index[0] if any([pd.notna(y) for y in x]) else '')
    inv = pd.merge(inv, top_brand, on=['dealer_id', 'date'], how='left', suffixes=['', '_top_brand'])
    inv = inv.rename({'brand_top_brand': 'top_brand'}, axis=1)

    # price
    print('\t price')
    inv['price_avg'] = inv.groupby(['dealer_id', 'date'])['price'].transform('mean')
    inv['price_std'] = inv.groupby(['dealer_id', 'date'])['price'].transform('std')

    # Collapse
    print('collapsing')
    drop_cols = ['price', 'brand', 'car_name', 'itemCondition', 'deal', 'home_delivery',
                'online_vendor', 'virtual_appointments', 'price', 'car_name','search_id',
                'color', 'vin', 'url']


    inv = inv[[c for c in inv.columns if c not in drop_cols]]

    dealer_day_inv = inv.drop_duplicates(['dealer_id', 'date']) #['dealer_id', 'date']

    dealer_day_inv['dealer_phone'] = dealer_day_inv['dealer_name'] + ' ' + dealer_day_inv['telephone'].astype(str)

    keywords = ['HHI', 'price_', 'num_', 'brand']
    keep_cols = ['url','dealer_name', 'telephone', 'dealer_rating', 'dealer_reviews', 'customer_id', 'make',
                 'online_seller', 'online_vendor', 'date', 'dealer_id', 'active_del', 'del_ratio', 'active_virt_apt',
                 ] + [c for c in dealer_day_inv.columns if any([k in c for k in keywords])]
    dealer_day_inv = dealer_day_inv[[c for c in keep_cols if c in dealer_day_inv.columns]]
    return dealer_day_inv

# import hashlib
# hash_object = hashlib.md5(b'Grand Subaru')
# print(hash_object.hexdigest())
# pattern = './listings/clean_listings_202?-??-??.csv'
def gen_dealer_day_inv(pattern = './listings/clean_listings_202?-??-??.zip'):
    ###### Convert each day's listings to dealers' daily inv ######
    paths = glob(pattern)    #sorted file paths
    paths.sort()
    print(f'identified:\n{paths}')

    for i in range(len(paths)):
        try:
            _, fname, _, suffix = parse_path(paths[i])
            if suffix == '.zip':
                inv, fname = read_zipped_csv(zip_path=paths[i])
            else:
                inv = pd.read_csv(paths[i])
            print('cleaning again because the cleaning functions has since been updated')
            inv = new_clean_listings(inv)
            date = fname.replace('clean_listings_', '').replace('.csv', '')

            out_path = f'./dealers/dealer_daily_inv_{date}.csv'
            if os.path.exists(out_path):
                continue
            print('listings_to_daily_inv')
            dealer_day_inv = new_listings_to_daily_inv(inv, date)   # Clean and save
            # Test: dealer_day[dealer_day['seller.name'] == 'Gentry Motor Company'][['active_local_del', 'date']]
            # Save
            dealer_day_inv.to_csv(out_path, index=False)
            #df_to_zip(dealer_day_inv, zip_path=out_path)
            print(f'{i}- date {date} dealer_daily_inv saved @ {out_path}.')
        except Exception as e:
            print('error:', e)
            raise e
    return
def old_gen_dealer_day_inv(pattern = './listings/clean_listings_202?-??-??.zip'):
    ###### Convert each day's listings to dealers' daily inv ######
    paths = glob(pattern)    #sorted file paths
    paths.sort()
    print(f'identified:\n{paths}')

    for i in range(len(paths)):
        try:
            # i=47
            _, fname, _, suffix = parse_path(paths[i])
            date = fname.replace('clean_listings_', '').replace('.csv', '')
            out_path = f'./dealers/dealer_daily_inv_{date}.csv'
            if os.path.exists(out_path):
                continue

            if suffix == '.zip':
                inv, fname = read_zipped_csv(zip_path=paths[i])
            elif suffix == '.csv':
                inv = pd.read_csv(paths[i])
            else:
                print(f'suffix {suffix} not supported')

            print('cleaning again because the cleaning functions has since been updated')
            inv = old_clean_listings(inv)
            print('listings_to_daily_inv')
            dealer_day_inv = old_listings_to_daily_inv(inv, date)   # Clean and save
            # Test: dealer_day[dealer_day['seller.name'] == 'Gentry Motor Company'][['active_local_del', 'date']]
            # Save
            dealer_day_inv.to_csv(out_path, index=False)
            #df_to_zip(dealer_day_inv, zip_path=out_path)
            print(f'{i}- date {date} dealer_daily_inv saved @ {out_path}.')
        except Exception as e:
            print('error:', e)
    return


def old_gen_all_dealer_days():
    print('Merging all all_dealer_days')
    all_dealer_days = pattern_merge('./dealers/dealer_daily_inv_202?-??-??.csv')
    all_dealer_days['dealer_id'] = all_dealer_days.groupby(['seller.name', 'seller.city']).ngroup()

    # Find the first date when online sales activated
    # all_dealerbooks['local_home_delivery'].apply(bool_to_int)
    # all_dealerbooks[all_dealerbooks['seller.name']=='Gentry Motor Company']
    # all_dealerbooks = dealer_day

    # firstLocalDel = all_dealer_days[all_dealer_days['active_local_del'] == 1].groupby(['dealer_id'], as_index=False)['date'].agg('min')
    # all_dealer_days = pd.merge(all_dealer_days, firstLocalDel, on=['dealer_id'], how='left', suffixes=['','_firstLocalDel'])
    # firstVirtualApt = all_dealer_days[all_dealer_days['active_virt_apt'] == 1].groupby(['dealer_id'], as_index=False)['date'].agg('min')
    # all_dealer_days = pd.merge(all_dealer_days, firstVirtualApt, on=['dealer_id'], how='left', suffixes=['', '_firstVirtualApt'])
    all_dealer_days['date_firstLocalDel'] = all_dealer_days[all_dealer_days['active_local_del'] == 1].groupby(['dealer_id'])['date'].transform(lambda x: min(x))
    all_dealer_days['date_firstVirtualApt'] = all_dealer_days[all_dealer_days['active_virt_apt'] == 1].groupby(['dealer_id'])['date'].transform(lambda x: min(x))

    print('Adding daily sales column')
    # sales = read_zipped_csv('./sales/allsales.zip')[0]
    sales = pattern_merge('./sales/allsales*.zip')

    sales = sales.rename({'data': 'date'}, axis=1)
    dealer_daily_revenue = sales.groupby(['seller.name', 'seller.city', 'date']).price.agg(total_sales=('price', 'sum'))
    dealers_merged = all_dealer_days.merge(dealer_daily_revenue, on=['seller.name', 'seller.city', 'date'], how='left')
    dealers_merged.loc[dealers_merged['total_sales'].isnull(), 'total_sales'] = 0

    print('Zipping')
    zip_path = './dealers/all_dealer_days.zip'
    df_to_zip(dealers_merged, zip_path)
    print(f'Complete. Saved @ {zip_path}')
    return

def new_gen_all_dealer_days():
    print('Merging all all_dealer_days')
    all_dealer_days = pattern_merge('./dealers/dealer_daily_inv_202?-??-??.???')
    all_dealer_days.columns
    all_dealer_days['dealer_id'] = all_dealer_days.groupby(['dealer_name', 'customer_id']).ngroup()

    # Find the first date when online sales activated
    # all_dealerbooks['local_home_delivery'].apply(bool_to_int)
    # all_dealerbooks[all_dealerbooks['seller.name']=='Gentry Motor Company']
    # all_dealerbooks = dealer_day

    ## use the combination of 'dealer_name', 'customer_id' to identify unique dealer
    firstLocalDel = all_dealer_days[all_dealer_days['active_del'] == 1].groupby(['dealer_name', 'customer_id'], as_index=False).date.agg('min')
    all_dealer_days = pd.merge(all_dealer_days, firstLocalDel, on=['dealer_name', 'customer_id'], how='left', suffixes=['','_firstDel'])
    firstVirtualApt = all_dealer_days[all_dealer_days['active_virt_apt'] == 1].groupby(['dealer_name', 'customer_id'], as_index=False).date.agg('min')
    all_dealer_days = pd.merge(all_dealer_days, firstVirtualApt, on=['dealer_name', 'customer_id'], how='left', suffixes=['', '_firstVirtualApt'])

    print('Adding daily sales column')
    # sales = read_zipped_csv('./sales/allsales.zip')[0]
    sales = pattern_merge('./sales/allsales*.zip')
    sales = sales.rename({'data': 'date'}, axis=1)
    dealer_daily_revenue = sales.groupby(['dealer_name', 'customer_id', 'date']).price.agg(total_sales=('price', 'sum'))
    dealers_merged = all_dealer_days.merge(dealer_daily_revenue, on=['dealer_name', 'customer_id', 'date'], how='left')
    dealers_merged.loc[dealers_merged['total_sales'].isnull(), 'total_sales'] = 0

    # [c for c in all_dealer_days.columns if 'dealer' in c]

    print('Zipping')
    zip_path = './dealers/all_dealer_days.zip'
    df_to_zip(dealers_merged, zip_path)
    print(f'Complete. Saved @ {zip_path}')
    return

def gen_address_book():
    d = pd.read_csv('./dealers/all_dealer_days.csv')
    d['raw_address'] = d['seller.streetAddress'] + ' ' + d['seller.city'] + ' ' + d['seller.state']
    raw_df = d[['raw_address']].drop_duplicates()
    geo = raw_df['raw_address'].apply(address_parts).apply(pd.Series)
    expanded_df = raw_df.merge(geo, left_index=True, right_index=True)
    out_path = './dealers/address_book.csv'
    expanded_df.to_csv(out_path, index=False)
    print(f"Geocoded and expanded df saved @ {out_path}")


def clean_raw_listings_with_same_name(pattern='./listings/listings_2*.zip'):
    print(f'cleaning up')
    ###### Convert each day's listings to dealers' daily inv ######
    paths = glob(pattern)  # sorted file paths
    paths.sort()
    print(f'identified:\n{paths}')

    for i in range(len(paths)):
        try:
            _, fname, _, suffix = parse_path(paths[i])
            if suffix == '.zip':
                raw_inv, fname = read_zipped_csv(zip_path=paths[i])
            else:
                raw_inv = pd.read_csv(paths[i])
            print('cleaning again because the cleaning functions has since been updated')

            try:
                inv_clean = new_clean_listings(raw_inv)

            except:
                # if the file is old, first try the traditional cleaning
                inv_clean = new_clean_listings(old_clean_listings(raw_inv))

            date = fname.replace('listings_', '').replace('.csv', '')

            out_path = f'./listings/clean_listings_{date}.csv'
            if os.path.exists(out_path):
                continue
            print('inv_clean')
            if len(inv_clean) > 0:
                inv_clean.to_csv(out_path, index=False)
                print(f'{i}- date {date} inv_clean saved @ {out_path}.')
            else:
                print('no listings found to clean')

        except Exception as e:
            print('error:', e)
            raise e

    return inv_clean

def old_gen_basic_dealer_book(pattern='./listings/listings_2*.zip'):
    try:
        dealer_days_new = pd.read_csv('./dealers/all_dealer_days.csv')
    except:
        dealer_days_new, fname = read_zipped_csv(f'./dealers/all_dealer_days.zip')

    cols = ['seller.city', 'seller.telephone', 'seller.streetAddress', 'seller.name', 'seller.state']
    dealer_days_new = dealer_days_new[cols]
    dealer_days_new = dealer_days_new.drop_duplicates(['seller.name', 'seller.city'])

    dealer_days_old = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/4- Dealerbook Manager/inputs/3_geocoded_merged_w_zipcode.csv')
    ### Combine the two and flag dealers that do not have an address from the new file:
    dealer_days_old['status'] = 'done'

    dealer_days_new = dealer_days_new.rename({'seller.name': 'dealer', 'seller.city': 'city_raw',
                                              'seller.streetAddress':'streetaddress', 'seller.state':'state_raw',
                                                  'seller.telephone':'telephone'}, axis=1)
    dealer_days_new['address'] = dealer_days_new['streetaddress'] + ', ' + dealer_days_new['city_raw']

    dealer_days_old['address']

    dd = dealer_days_old.append(dealer_days_new)
    dd['id'] = dd['dealer'] + dd['city_raw']
    dd = dd.sort_values(['address', 'status'], na_position='last').drop_duplicates(['address'], keep='first')
    dd.to_csv('./dealers/basic_dealer_book.csv')
    return


def gen_basic_dealer_book(pattern='./listings/listings_2*.zip'):
    try:
        dealer_days_new = pd.read_csv('./dealers/all_dealer_days.csv')
    except:
        dealer_days_new, fname = read_zipped_csv(f'./dealers/all_dealer_days.zip')

    pd.options.display.float_format = '{:.2f}'.format
    dealer_days_new.loc[dealer_days_new['dealer_name']=='iAuto of Santa Ana']

    cols = ['url', 'dealer_name', 'telephone', 'dealer_rating', 'dealer_reviews', 'customer_id', 'online_seller', 'dealer_id', 'date_firstDel', 'date_firstVirtualApt']
    dealer_days_new = dealer_days_new.sort_values(['dealer_name', 'date'], ascending=False, na_position='last')
    dealer_days_new.columns
    dealer_days_new[['dealer_name', 'date']]

    dealer_days_new = dealer_days_new[cols]
    dealer_days_new = dealer_days_new.drop_duplicates(['dealer_id'], keep='last')
    dealer_days_new['tel3'] = dealer_days_new['telephone'].astype(str).str[0:3]
    # dealer_days_new['telephone'] = \
    #     dealer_days_new['telephone'].astype('int64', errors='ignore').round(4)
    #

    # dealer_days_old = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/4- Dealerbook Manager/inputs/3_geocoded_merged_w_zipcode.csv')
    dealer_days_old = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/1_2_3_Batches/dealers/dealer_daily_inv_2021-02-13.csv')
    ### Combine the two and flag dealers that do not have an address from the new file:
    dealer_days_old['status'] = 'done'
    dealer_days_old = dealer_days_old.rename({'seller.name': 'dealer_name', 'seller.telephone': 'telephone'}, axis=1)
    dealer_days_old['telephone'] = dealer_days_old['telephone'].str.replace('(', '').str.replace(')', '').str.replace('-', '').str.replace(' ', '').astype(float)
    dealer_days_old['tel3'] = dealer_days_old['telephone'].astype(str).str[0:3]
    dealer_days_new['telephone'] = dealer_days_new['telephone'].fillna(0)

    dealer_days_old = dealer_days_old.drop_duplicates(['dealer_name', 'seller.city'])
    dealer_days_old.loc[dealer_days_old.duplicated(['dealer_name', 'tel3']), ['dealer_name', 'seller.city']].sort_values(['dealer_name'])
    dealer_days_old[dealer_days_old.dealer_name=='1st Choice Auto']
    dealer_days_old = dealer_days_old[dealer_days_old.dealer_name.notna()]
    dealer_days_old['telephone'] = dealer_days_old['telephone'].fillna(0)

    # ## merge to check which new dealers do not have location information
    # x = dealer_days_new[[ 'dealer_name', 'dealer_id', 'telephone', 'tel3', 'key']].merge(
    #     dealer_days_old[['dealer_name', 'telephone', 'seller.city', 'tel3', 'key']], on=['key'], how='left')
    #### NEW mege approach. First try the most stringent (name+telephone). Then try onlt tel3, then only name.
    ## merge to check which new dealers do not have location information
    x = dealer_days_new[['url', 'dealer_name', 'dealer_id', 'telephone', 'tel3']].merge(
        dealer_days_old[['dealer_name', 'telephone', 'seller.city']], on=['dealer_name', 'telephone'], how='left', suffixes=['','_y'])
    x[x['seller.city'].isna()]['dealer_name']

    # keep whatever that was not matched, and try matching with easier criteria (only dealer_name & tel3)
    y = x[x['seller.city'].isna()]
    y = y.drop(['seller.city'], axis=1)
    y = y.merge(dealer_days_old[['dealer_name', 'seller.city', 'tel3']], on=['dealer_name', 'tel3'], how='left', suffixes=['','_y'])
    y[y['seller.city'].isna()]['dealer_name']
    y.columns

    # keep whatever that was not matched, and try matching with easier criteria (only dealer_name)
    z = y[y['seller.city'].isna()]
    z = z.drop(['seller.city'], axis=1)
    z.columns
    z = z.merge(dealer_days_old[['dealer_name', 'telephone', 'seller.city', 'tel3']], on=['dealer_name'], how='left', suffixes=['','_y'])
    z[z['seller.city'].isna()]['dealer_name']
    print( z[z['seller.city'].isna()])
    # save unmatched so be done manually later.
    z.to_csv('./unmatched_dealers.csv', index=False)
    z[z.url.isna()]

    w = x[x['seller.city'].notna()].append(y).append(z)
    w.loc[w.dealer_id.duplicated(), ]
    w = w.drop_duplicates()
    w.drop_duplicates().to_csv('./matched_dealers.csv', index=False)


    # x[x.dealer_name_x == '1st Choice Auto']
    # x[x['dealer_name_x'].duplicated()].sort_values('dealer_name_x')
    # dealer_days_old.loc[dealer_days_old['dealer_name'].str.contains('Hyman'), ['dealer_name', 'telephone']]
    # dealer_days_new['dealer_name'].isin(dealer_days_old['dealer_name']).sum()
    ## find


    # CODE THAT STILL NEEDS EDITING
    #
    # dealer_days_new[['dealer_name']]
    #
    # len(dealer_days_new['dealer_name'])
    # dealer_days_old.columns
    #
    #
    #
    # # dealer_days_new['address'] = dealer_days_new['streetaddress'] + ', ' + dealer_days_new['city_raw']
    #
    # # dealer_days_old['address']
    #
    # dd = dealer_days_old.append(dealer_days_new)
    # dd['id'] = dd['dealer'] + dd['city_raw']
    # dd = dd.sort_values(['address', 'status'], na_position='last').drop_duplicates(['address'], keep='first')
    # dd.to_csv('./dealers/basic_dealer_book.csv')
    return


if __name__ == "__main__":
    Dir = '/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/2- AutoDataLoop'
    os.chdir(Dir)
    print('working dir changed:' + os.getcwd())

    print('Argument List:', str(sys.argv))

    if 'clean_listings' in sys.argv:
        clean_all_listings()

    if 'sales' in sys.argv:
        filter_VINs(skip_if_exists=True)       # clean_listings => VINs (only keep vins)
        sold_json = find_sold_vins(skip_if_exists=True)    # VINs => sold.json
        gen_all_sales(sold_json)

    if 'day_level' in sys.argv:
        print('Generating day-level inventory...')
        gen_dealer_day_inv(pattern='./listings/clean_listings_202?-??-??.zip')
        sys.exit("Day_level complete.")

    if 'old_day_level' in sys.argv:
        print('Generating day-level inventory...')
        old_gen_dealer_day_inv(pattern='./listings/clean_listings_202?-??-??.csv')
        sys.exit("Day_level complete.")

    if 'old_all_dealer_days' in sys.argv:
        old_gen_all_dealer_days()

    if 'all_dealer_days' in sys.argv:
        new_gen_all_dealer_days()

    if 'basic_dealerbook' in sys.argv:
        gen_basic_dealer_book()

    if 'reports' in sys.argv:
        print('GENERATING REPORTS')
        gen_all_reports()

    print('complete')



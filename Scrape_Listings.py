import os, sys
import pandas as pd, numpy as np
from scrape_utilities.extractor import listing_extractor, dealer_info_extractor
from scrape_utilities.TorRequest import TorRequest, ip_stats, RegularRequest
from functools import partial
from math import ceil
from cleaning.file_management import pattern_merge, pattern_zip, pattern_move, rm_dir, parse_path
from time import time, sleep
from multiprocessing import get_context
from cleaning.time_management import eastern_time
from Listings_Cleaner import gen_listings_report, old_clean_listings, bool_to_int, new_clean_listings, old_clean_col_names, gen_dealer_day_inv
from random import random
from scrape_utilities.EmailSender import email_with_report

#
# # Test Cleaning
# y = pd.read_csv('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop/listings/listings_2020-12-26.csv')
# listings = y[1:10000]
# z = clean_listings(listings)
# z[['deal', 'url']]
# y['deal']
# z.to_csv('./z.csv', index=False)



def Scrape_Listings(Guide, M='', use_tor=True):    # M is the partition number as a file name extensions for organization
    '''# Part 1: Extracting the list of cars (max n=50x100=5000)'''
    listings = pd.DataFrame()
    N = len(Guide)
    start_time = time()
    c = 0
    session = False
    keep_session = False

    for i in Guide.index:
        e = Guide.loc[i, 'Result']
        try:
            if e in ['complete', 'no matches message']:
                continue
        except Exception as EE:
            print(f'e = {e} \n {str(EE)}')
            pass
        search_id = Guide.loc[i, 'search_id']
        URL = Guide.loc[i, 'url']

        try:
            if use_tor:
                html, session, keep_session = TorRequest(URL, session=session, balanced_ip_use=True)
            else:
                html, session, keep_session = RegularRequest(URL)
                sleep(random())

            new_df, result = listing_extractor(html)

            ## Store some notes about the data extraction
            notes = ''
            if result == 'no matches message':
                with open(f'./listings/warnings.txt', 'a+') as f:
                    f.write('\nno matches message: '+ str(URL))
                    notes, result = result, 'complete'
            if result == 'too many':
                with open('./listings/warnings.txt', 'a+') as f:
                    f.write('\ntoo many: ' + str(URL))
                    notes, result = result, 'complete'
            Guide.loc[i, 'Result'] = result
            Guide.loc[i, 'Notes'] = notes

            c += 1

        except Exception as E:
            new_df = pd.DataFrame()
            Guide.loc[i, 'Result'] = E

            print(f'{i} {URL}')
            print(f'error {E}')

        new_df['search_id'] = search_id
        listings = listings.append(new_df)
        progress = int(c/N*100)
        min_elapsed = int((time()-start_time)/60)

        if keep_session:
            session_Reuse_Msg = '  Session Reused'
        else:
            session_Reuse_Msg = ''

        print(f'Partition {M} URL {i} search_id {search_id} complete. ({progress}%)  [{min_elapsed} min elapsed]{session_Reuse_Msg}')

        #time.sleep(random.randint(1, 3))
        if i % 100 == 0:
            listings.to_csv(f'./listings/listings{M}.csv', index=0)
            Guide.to_csv(f'./guide/Guide_With_Pages_Processed{M}.csv', index=0)
            if use_tor: ip_stats(pattern='./ip_check/ip_repo/*.txt', output='./ip_check/ip_repo/ip_statistics.csv')

    listings.reset_index(inplace=True, drop=True)
    listings.loc[0,'download_time'] = eastern_time() #add download time to the first row only.
    listings.to_csv(f'./listings/listings{M}.csv', index=0)
    Guide.to_csv(f'./guide/Guide_With_Pages_Processed{M}.csv', index=0)


def Scrape_Generic(urls_df, extractor, output_path='', processed_path='', use_tor=False, reference_cols=[]):
    '''# Part 1: Extracting the list of cars (max n=50x100=5000)'''
    # parent, _, _, _ = parse_path(output_path)
    # processed_path = parent.joinpath('urls_processed.csv')

    extracted_data = pd.DataFrame()
    N = len(urls_df)
    start_time = time()
    c = 0
    session = False
    keep_session = False
    if 'Result' not in urls_df.columns:
        urls_df['Result'] = ''

    for i in urls_df.index:
        e = urls_df.loc[i, 'Result']
        try:
            if e == 'complete':
                continue
        except Exception as EE:
            print(f'e = {e} \n {str(EE)}')
            pass
        URL = urls_df.loc[i, 'url']

        try:
            if use_tor:
                html, session, keep_session = TorRequest(URL, session=session, balanced_ip_use=True)
            else:
                html, session, keep_session = RegularRequest(URL)
                sleep(random())

            new_df = extractor(html)
            ## Add some info from the original guide dataset for ease of reference
            new_df['url'] = URL
            for cl in reference_cols:
                new_df[cl] = urls_df.loc[i, cl]
            if len(new_df) >= 0:
                urls_df.loc[i, 'Result'] = 'complete'
            c += 1

        except Exception as E:
            new_df = pd.DataFrame()
            urls_df.loc[i, 'Result'] = E

            print(f'{i} {URL}')
            print(f'error {E}')

        extracted_data = extracted_data.append(new_df)
        progress = int(c/N*100)
        min_elapsed = int((time()-start_time)/60)

        if keep_session:
            session_Reuse_Msg = '  Session Reused'
        else:
            session_Reuse_Msg = ''

        print(f'URL {i} complete. ({progress}%)  [{min_elapsed} min elapsed]{session_Reuse_Msg}')

        #time.sleep(random.randint(1, 3))
        if i%100==0:
            extracted_data.to_csv(output_path, index=0)
            urls_df.to_csv(processed_path, index=0)
            if use_tor: ip_stats(pattern='./ip_check/ip_repo/*.txt', output='./ip_check/ip_repo/ip_statistics.csv')

    extracted_data.reset_index(inplace=True, drop=True)
    extracted_data.loc[0,'download_time'] = eastern_time() #add download time to the first row only.
    extracted_data.to_csv(output_path, index=0)
    urls_df.to_csv(processed_path, index=0)
    return
# M =1
def Scrape_One_Partition(M, Guide, Max_Threads, use_tor=True): #M is the partition number
    N = len(Guide)
    size = ceil(N / Max_Threads)
    ll = size * M
    uu = min(N, size * (M+1))
    print(f'Thread {M} started: Guide[{ll}:{uu}]')
    Guide_chunk = Guide[ll:uu].copy()
    Scrape_Listings(Guide_chunk, M=str(M), use_tor=use_tor)

def Scrape_Failed(M, Guide, Max_Threads, use_tor=True): #M is the partition number
    N = len(Guide)
    size = ceil(N / Max_Threads)
    ll = size * M
    uu = min(N, size * (M+1))
    print(f'Thread {M} started: Guide[{ll}:{uu}]')
    Guide_chunk = Guide[ll:uu].copy()
    Scrape_Listings(Guide_chunk, M='_F'+str(M), use_tor=use_tor)

    #listings.to_csv(f'./listings/listings{M}.csv', index=0)

def Scrape_Dealer_info(ext):
    dealer_guide = pd.read_csv(f'./listings/clean_listings_{ext}.csv')
    cols = [c for c in dealer_guide.columns if c in ['dealer_name', 'customer_id', 'online_vendor', 'url', 'telephone']]
    dealer_guide = dealer_guide[cols]
    # for each dealer, find one url
    dealer_guide = dealer_guide.drop_duplicates(['customer_id', 'dealer_name'])
    #visit each link & extract
    dealer_guide['url'] = 'https://www.cars.com/vehicledetail' + dealer_guide['url']
    dealer_guide.to_csv(f'./dealers/dealer_guide_{ext}.csv')
    Scrape_Generic(dealer_guide, extractor=dealer_info_extractor, output_path=f'./dealers/dealers_info_{ext}.csv',
                   processed_path=f'./dealer_guide_processed_{ext}.csv', reference_cols=['dealer_name','customer_id'])
    return

def drop_duplicate_items(df):
    if len(df) == 0:
        return df
    # problem: there could be two instances of a url: one complete one failed.
    # categorical sort, bring "complete" to top, keep the first.
    df['Result'] = pd.Categorical(df['Result'], ["complete", ""]) #values other than "complete"/"" will be Nan
    df = df.sort_values(['url', 'Result']).drop_duplicates('url', keep='first')
    df.loc[pd.isna(df.Result), 'Result'] = ""
    df['Result'] = df['Result'].astype(str)
    print(f'''{len(df[df.Result != 'complete'])} failed items''')
    return df

def drop_complete_items(df):
    df = drop_duplicate_items(df)
    if len(df) > 0:
        df = df[df['Result'] != 'complete']
    return df

def Update_Guide():
    # # # Append all files & clean processed records and extract failed ones.
    Guide_With_Pages_Processed = pattern_merge(pattern='./guide/Guide_With_Pages_Processed*.csv', move_to_folder='./guide/hist', drop_duplicates=True)
    Cleaned = Failed = pd.DataFrame()
    if len(Guide_With_Pages_Processed) == 0:
        print(f'Guide_With_Pages_Processed is empty')
    else:
        Cleaned = drop_duplicate_items(Guide_With_Pages_Processed)
        Cleaned.to_csv('./guide/Guide_With_Pages_Processed.csv', index=False)
        Failed = drop_complete_items(Guide_With_Pages_Processed)
        Failed.to_csv('./guide/Failed.csv', index=False)

    return Cleaned, Failed

def clean_only(ext=''):
    print(f'cleaning up')
    Guide, Failed = Update_Guide()

    all_listings = pattern_merge(pattern='./listings/listings*.csv', output=f'./listings/listings_{ext}.csv',
                                 move_to_folder='./listings/hist', drop_duplicates=True)

    # Clean
    try:
        all_listings_clean = new_clean_listings(all_listings)
    except:
        # if the file is old, first try the traditional cleaning
        all_listings_clean = new_clean_listings(old_clean_listings(all_listings))

    if len(all_listings_clean) > 0:
        all_listings_clean.to_csv(f'./listings/clean_listings_{ext}.csv', index=0)
    else:
        print('no listings found to clean')

    # print('zipping ...')
    # pattern_zip('./listings/*.csv', move_to_folder='./recycle_bin')
    print('moving history ...')
    pattern_move('./listings/hist/*', move_to_folder='./recycle_bin')

    # Date warning.txt file
    warning_path = './listings/warnings.txt'
    new_warning_path = f'./listings/warnings_{ext}.txt'
    if os.path.exists(warning_path):
        os.rename(warning_path, new_warning_path)
        print(f'{warning_path} renamed to {new_warning_path}')
    else:
        print(f'{warning_path} not found')

    # Report
    report = gen_listings_report(all_listings_clean=all_listings_clean, warning_path=new_warning_path)
    report.to_csv(f'./listings/report_listings_{ext}.csv', index=False)
    print(f'report saved @ ./listings/report_listings_{ext}.csv')

    return all_listings_clean


def zip_only():
    print('zipping ...')
    pattern_zip('./listings/listings_*.csv', move_to_folder='./recycle_bin')
    pattern_zip('./listings/clean_listings_*.csv', move_to_folder='./recycle_bin')
    pattern_zip('./dealers/dealer_daily_inv_*.csv', move_to_folder='./recycle_bin')
    # pattern_zip('./listings/hist/*.csv', move_to_folder='./recycle_bin')
    return


def move_to_onedrive_and_erase(empty_recycle_bin=True):
    print('moving ...')
    pattern_move('./listings/listings_*-*-*.zip', move_to_folder='~/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Raw Listings')
    pattern_move('./listings/clean_listings_*-*-*.zip', move_to_folder='~/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Daily Listings')
    if empty_recycle_bin:
        print('emptying recycle bin ...')
        rm_dir('./recycle_bin')
    return


if __name__ == '__main__':
    ''' On mac run: OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES caffeinate python3 Scrape_Listings.py'''
    if 't_1' in sys.argv:
        Date = eastern_time('%Y-%m-%d', delta=-1)
    else:
        Date = eastern_time('%Y-%m-%d', delta=0)   # Yesterday: Date_m1 = eastern_time('%Y-%m-%d', delta=-1)
    print(f'Date set to {Date}')

    if 'no_email' in sys.argv:
        pass
    else:
        email_with_report(Date, subject='Start')

    if 'zip_only' in sys.argv:
        zip_only()
        sys.exit("Zipping complete.")

    if 'clean_only' in sys.argv:
        clean_only(ext=Date)
        sys.exit("Cleaning complete.")

    if 'email_only' in sys.argv:
        print('sending email...')
        email_with_report(Date, subject='report')
        sys.exit("Cleaning complete.")

    if 'put_away' in sys.argv:
        move_to_onedrive_and_erase(empty_recycle_bin=True)
        sys.exit("move complete and recycle bin empty.")

    if 'use_tor' in sys.argv:
        print('Will use tor')
        use_tor = True
    else:
        print('Will NOT use tor')
        use_tor = False

    if 'resume' in sys.argv:
        print(f'resuming previous task')
        Guide, Failed = Update_Guide()

        pattern_merge(pattern='./listings/listings[0-9]*.csv', output='./listings/listings000.csv', move_to_folder='./listings/hist', drop_duplicates=True)
        Guide = pd.read_csv('./guide/Guide_With_Pages_Processed.csv')
    else:
        Guide = pd.read_csv('./guide/Guide_With_Pages.csv')

    if 'test' in sys.argv:
        # slice if its a test
        print(f'only keeping the first 10 rows in Guide')
        Guide = Guide[0:10]

    if 'section' in sys.argv:
        Section = input('section:')
        print(f'filtering for Section {Section}')
        Guide = Guide[Guide['Section'] == Section]

    Max_Threads = 6
    with get_context("spawn").Pool(Max_Threads) as pool:
        func = partial(Scrape_One_Partition, Guide=Guide, Max_Threads=Max_Threads, use_tor=use_tor)
        pool.map(func, range(Max_Threads))
        pool.close()
        pool.join()


    ## Update
    Guide, Failed = Update_Guide()
    pattern_merge(pattern='./listings/listings*.csv', output='./listings/listings.csv', move_to_folder='./listings/hist', drop_duplicates=True)

    # Scrape Failed Ones
    with get_context("spawn").Pool(Max_Threads) as pool:
        func = partial(Scrape_Failed, Guide=Failed, Max_Threads=Max_Threads, use_tor=use_tor)
        pool.map(func, range(Max_Threads))
        pool.close()
        pool.join()

    clean_listings = clean_only(ext=Date)
    # # # Append all files
    Guide, Failed = Update_Guide()

    # ## Scrape Dealer Information
    # print('Scraping dealer information...')
    # Scrape_Dealer_info(ext=Date)

    ## Generate day-level dataset
    print('Generating day-level inventory...')
    gen_dealer_day_inv(pattern=f'./listings/clean_listings_{Date}.csv')


    if 'zip' in sys.argv:
        zip_only()

    if 'no_email' in sys.argv:
        pass
    else:
        email_with_report(Date, subject='Code Complete & Zipped')





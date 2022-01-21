import pandas as pd
from functools import partial
from multiprocessing import Pool
from time import time, sleep
from math import ceil

from cleaning.file_management import pattern_merge
from extractor import feature_extractor
from TorRequest import TorRequest, ip_stats

# Max_Threads = 10
# listings = pd.read_csv('./listings/all_listings.csv')
# all_listings = pd.read_csv('./features/listings_processed.csv')
# M=0
# i=0

def drop_duplicate_items(df):
    # problem: there could be two instances of a url: one complete one failed.
    # categorical sort, bring "complete" to top, keep the first.
    df['result'] = pd.Categorical(df['result'], ["complete", ""]) #values other than "complete"/"" will be Nan
    df = df.sort_values(['vin', 'result']).drop_duplicates('vin', keep='first')
    df.loc[pd.isna(df.result), 'result'] = ""
    print(f'''{len(df[df.result != 'complete'])} failed items''')
    return df


def Scrape_Features(M, Max_Threads, all_listings, ext=''):  # M is the partition number as a file name extensions for organization
    N = len(all_listings)
    size = ceil(N / Max_Threads)
    ll = size * M
    uu = min(N, size * (M+1))
    print(f'Partition {M} started: listings[{ll}:{uu}]')

    features_df = pd.DataFrame()
    listings = all_listings[ll:uu].copy()
    if 'result' not in listings.columns:
        listings['result'] = ''
    start_time = time()
    c = 0
    session = False

    for i in listings.index:
        vin = listings.loc[i,'vin']
        e = listings.loc[i,'result']
        if e == 'complete': continue
        url = listings.loc[i, 'url']
        new_df = pd.DataFrame()

        try:
            html, session, keep_session = TorRequest(url, session=session, balanced_ip_use=True)
            features = feature_extractor(html)
            # new_df = pd.merge(flat1, flat2)
            # new_df.index = [i]
            new_df.loc[i,'vin'], new_df.loc[i,'features'] = vin,str(features)
            listings.loc[i, 'result'] = 'complete'

        except Exception as e:
            features, flat1, flat2 = {}, pd.DataFrame() ,pd.DataFrame()
            new_df.loc[i,'vin'],new_df.loc[i,'features'] = vin,str(features)
            listings.loc[i,'result'] = e
            print(e)

        features_df = features_df.append(new_df)
        c += 1
        if listings.loc[i, 'result'] == 'complete':
            print(f'Partition {M} \t listing {i}/{max(listings.index)} \t complete ({int(c/(uu-ll)*1000)/10}%). [{int((time()-start_time)/60)} min elapsed]')
        #time.sleep(random.randint(1, 3))
        if i%100==0:
            features_df.to_csv(f'./features/features{ext}{M}.csv', index=0)
            listings.to_csv(f'./listings/listings_processed{ext}{M}.csv', index=0)
            # IP statistics
            ip_stats(pattern='./ip_check/ip_repo/*.txt', output='./ip_check/ip_repo/ip_statistics.csv')

    features_df.to_csv(f'./features/features{ext}{M}.csv', index=0)
    listings.to_csv(f'./listings/listings_processed{ext}{M}.csv', index=0)

    # IP statistics
    ip_stats(pattern='./ip_check/ip_repo/*.txt', output='./ip_check/ip_repo/ip_statistics.csv') #new_folder='./ip_check/ip_repo/hist'


def drop_duplicate_listings(df):
    # problem: there could be two instances of a url: one complete one failed.
    # categorical sort, bring "complete" to top, keep the first.
    df['result'] = pd.Categorical(df['result'], ordered=True, categories = ["complete", ""]) #values other than "complete"/"" will be Nan
    df = df.sort_values(['URL_end', 'result']).drop_duplicates('vin', keep='first')
    df.loc[pd.isna(df.Result), 'result'] = ""
    df['result'] = df['result'].astype(str)
    print(f'''{len(df[df.Result != 'complete'])} failed items''')
    return df


def Update_Listings():
    # # # Append all files & clean processed records and extract failed ones.
    listings = pattern_merge(pattern='./listings/listings_processed*.csv', output='./listings/listings_processed.csv',
                             move_to_folder='./listings/hist', drop_duplicates=True)
    Cleaned = drop_duplicate_items(listings)
    Cleaned.to_csv('./listings/listings_processed.csv', index=False)
    Failed = Cleaned[~(Cleaned['result'] == 'complete')]
    Failed.to_csv('./listings/Failed.csv', index=False)
    return Cleaned, Failed

if __name__ == '__main__':
    all_listings = pd.read_csv('./listings/listings_clean.csv').drop_duplicates('vin')[['vin','url','result']]

    TH = 15
    pool = Pool(TH)
    Max_Threads = TH
    func = partial(Scrape_Features, all_listings=all_listings, Max_Threads=Max_Threads)
    pool.map(func, range(Max_Threads))
    pool.close()
    pool.join()

    # Combine files
    pattern_merge(pattern='./features/features[0-9]*.csv', output='./features/features_merged.csv', move_to_folder='./features/hist')

    Cleaned, Failed = Update_Listings()
    # pattern_merge(pattern='./listings/listings_processed*.csv', output='./listings/listings_processed_merged.csv', new_folder='./listings/hist')

    #
    # # Scrape Failed Ones
    # shutil.copy('./listings/listings_processed_merged.csv', './listings/listings_processed_F_M.csv') # get a copy for backup

    for i in range(5):
        Failed = pd.read_csv(f'./listings/Failed.csv')[['vin','url','result']]
        num_failed = len(Failed) # Count failed ones
        if num_failed>0:
            print(f'\n\n{num_failed} items failed. Re-scraping attempt {i+1} ...\n\n')
            sleep(1)

            pool = Pool(TH)
            Max_Threads = TH
            func = partial(Scrape_Features, all_listings=Failed, Max_Threads=Max_Threads, ext='_F')
            pool.map(func, range(Max_Threads))
            pool.close()
            pool.join()

            pattern_merge(pattern='./features/features_F*.csv', output='./features/features_F_merged.csv', move_to_folder='./features/hist')
            Cleaned, Failed = Update_Listings()

    # Final Merge: First round and failed ones:
    features_all_merged = pattern_merge(pattern='./features/features*.csv').drop_duplicates()
    bad_features = ['{}', '''{'basic': '', 'det': '', 'top': {}}''', '''{'notavailable': 'sold'}''']
    for f in bad_features:
        features_all_merged = features_all_merged[features_all_merged.features != f].sort_values('vin')
    features_all_merged.loc[features_all_merged.duplicated('vin', keep=False), 'duplicate'] = 1
    features_all_merged.to_csv('./features/features_all_merged.csv')

    features_all_merged.to_csv('./features/features_all_merged.csv', index=False)
    Cleaned, Failed = Update_Listings()

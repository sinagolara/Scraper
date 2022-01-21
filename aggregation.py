
import pandas as pd
from pandas.errors import EmptyDataError
from glob import glob
import os
import shutil
from json_flatten import flatten
from pathlib import Path


def pattern_merge(pattern, output ='', new_folder='', drop_duplicates=False):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')
    merged = pd.DataFrame()
    for path in glob(pattern):
        try:
            df = pd.read_csv(path, low_memory=False)
            print(path, 'read')
        except EmptyDataError:
            print(f'empty data @ {path}')
            df = pd.DataFrame()
        merged = merged.append(df)
        fname = os.path.basename(path)
        if new_folder != '':
            shutil.move(path,os.path.join(new_folder, fname))
    if drop_duplicates:
        merged = merged.drop_duplicates()
    if output != '':
        merged.to_csv(output, index=False)
        print('merged @', output)
    return merged




path1 = '/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/'
#listings = pattern_merge(pattern=pattern, output=path + '/listings_all.csv', drop_duplicates=True)
pattern = path1 + 'Sec*/listings_clean.csv'

merged = pd.DataFrame()
for path2 in glob(pattern):

    df = pd.read_csv(path2)

    folder = Path(path2).parent
    add = os.path.basename(folder)

    cols = df.columns
    cols2 = [c for c in cols if c not in ['link', 'payment_list.0', 'payment_list.1',
       'deal', 'mmyt', 'hot_badge', 'local_home_delivery',
       'national_home_delivery', 'virtual_appointments']]
    df = df [cols2]
    df.to_csv(str(folder) + f'/{add}_listings_cleaned.csv', index=False)

    vin_num = len(df['vin'])
    vin_unique = len(df['vin'].drop_duplicates())


    new_folder = str(folder.parent) + '/Backup/hist'
    fname = os.path.basename(path2)
    new_path = os.path.join(new_folder, fname)
    shutil.move(path2, new_path)

    print(f'{path2} read. total VINs {vin_num}, unique {vin_unique}. Moved {path2} to {new_path}')



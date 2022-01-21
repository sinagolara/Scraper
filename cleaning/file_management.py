
import pandas as pd
from pandas.errors import EmptyDataError
from glob import glob
import os
import shutil
from json_flatten import flatten
import zipfile
import pathlib

# pattern = './listings/clean_listings*.csv'


def pattern_merge(pattern, output ='', move_to_folder='', drop_duplicates=False):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')

    merged = pd.DataFrame()
    paths = glob(pattern)
    paths.sort()

    oneFileSameOutput = False
    if len(paths) == 1:
        path = paths[0]
        if os.path.realpath(path) == os.path.realpath(output):
            oneFileSameOutput = True
            print('there is only one match with the same name as output, nothing else to merge.')

    for path in paths:
        try:
            parent, fname, stem, suffix = parse_path(path)
            # print(f'Identified:\t {path}')
            if suffix == '.zip':
                df, fname = read_zipped_csv(zip_path=path)
            elif suffix == '.csv':
                df = pd.read_csv(path)
            else:
                print(f'suffix {suffix} not supported')


        except EmptyDataError:
            print(f'empty data @ {path}')
            df = pd.DataFrame()

        merged = merged.append(df)
        print(f'\t\t{path} appended')

        if move_to_folder != '':
            if not os.path.exists(move_to_folder):
                os.makedirs(move_to_folder)
            shutil.move(path, os.path.join(move_to_folder, fname))

    if drop_duplicates:
        merged = merged.drop_duplicates()
    if output != '' and ~oneFileSameOutput:
        merged.to_csv(output, index=False)
        print('merged @', output)

    if len(merged) == 0:
        print(f'pattern merge returned no records. len(merged) == {len(merged)}')

    return merged

def parse_path(path):
    PATH = pathlib.Path(path)
    return PATH.parent, PATH.name, PATH.stem, PATH.suffix   #par, name, stem, suffix = parse_path(path)

def pattern_move(pattern, move_to_folder=''):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')
    for path in glob(pattern):
        fname = os.path.basename(path)

        if move_to_folder != '':
            if not os.path.exists(move_to_folder):
                os.makedirs(move_to_folder)
            dest = os.path.join(move_to_folder, fname)
            shutil.move(path, dest)
            print(f'{path} moved to {dest}')
        else:
            print(f'{path} was not moved to {dest}')
    return

def parse_path(path):
    PATH = pathlib.Path(path)
    return PATH.parent, PATH.name, PATH.stem, PATH.suffix   #par, name, stem, suffix = parse_path(path)


def pattern_zip(pattern, move_to_folder=''):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')
    for path in glob(pattern):
        PATH = pathlib.Path(path)
        zip_PATH = PATH.with_suffix('.zip') #change suffix to zip

        with zipfile.ZipFile(zip_PATH, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as myzip:
            myzip.write(path, arcname=PATH.name)

        if move_to_folder != '':
            move_folder_PATH = pathlib.Path(move_to_folder)
            move_PATH = move_folder_PATH.joinpath(PATH.name)
            if not move_folder_PATH.is_dir():
                os.makedirs(move_folder_PATH)
            #PATH.rename()
            shutil.move(PATH, move_PATH)
        print(f'{PATH}\t=>\tcompressed@ {zip_PATH}\t=>\tmoved to {move_PATH}')
    return




def unzip_file(fpath, extract = 'All'):
    zip_PATH = pathlib.Path(fpath)
    print(f'Unzipping {zip_PATH}')
    with zipfile.ZipFile(zip_PATH, mode='r') as myzip:
        name_list = myzip.namelist()
        name_list = [n for n in name_list if '__MACOSX/' not in n]
        print(f'\tFiles found in the archive:\t{name_list}')
        if extract == 'All':
            print(f'\tExtracting all ...')
            myzip.extractall(zip_PATH.parent)  # extract all
        if extract == 'CSVs':
            for f in [n for n in name_list if n.endswith('.csv')]:
                print(f'Extracting {f} ...')
                myzip.extract(zip_PATH.parent.joinpath(f), path=zip_PATH.parent)  # if filename is known
    return
#
# os.chdir('/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Batch03')
# pattern = './listings/clean_listings_202?-??-??.zip'
def pattern_unzip(pattern, move_to_folder=''):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')
    paths = glob(pattern)
    paths.sort
    for path in paths:
        unzip_file(path, extract='All')
    return



def df_to_zip(df, zip_path):

    zip_PATH = pathlib.Path(zip_path)
    csv_PATH = zip_PATH.with_suffix('.csv')

    df.to_csv(csv_PATH, index=False)

    with zipfile.ZipFile(zip_PATH, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as myzip:
        myzip.write(csv_PATH, arcname=csv_PATH.name)

    os.remove(csv_PATH)
    print(f'Dataframe saved @ {zip_PATH}')
    return


def read_zipped_csv(zip_path):
    zip_PATH = pathlib.Path(zip_path)
    myzip = zipfile.ZipFile(zip_PATH, mode='r')
    name_list = myzip.namelist()
    name_list = [n for n in name_list if '__MACOSX/' not in n]

    if len(name_list) != 1:
        print(f'the size of archive is not one: {name_list}')
        raise Exception
    csv_name = myzip.namelist()[0]
    csv = pd.read_csv(myzip.open(csv_name)) #, nrows=1000
    print(f'\t reading {csv_name} complete (from {myzip.namelist()}) @ {zip_PATH.name} ')
    return csv, csv_name


def rm_dir(dir_path):
    shutil.rmtree(dir_path)
    print(f'{dir_path} removed')


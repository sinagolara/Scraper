import csv
import fnmatch
import os
import re
import zipfile
from urllib.parse import urlparse

import pandas as pd
import requests
import sys
from cleaning.time_management import eastern_time

from bs4 import BeautifulSoup
from pandas import json_normalize


# When dealer details couldn't be fetched, this function helps check random urls of the dealer
# Condition include having same dealer_name and telephone
def listing_similar_dealer_urls_extractor(file, dealername, phonenumber):
    # Input the original csv file which has all the records
    with open(file, mode='r') as actual_input_csvfile:
        actual_df = pd.read_csv(actual_input_csvfile)
        other_url_listings = []
        dealer_df = json_normalize(other_url_listings)
        for i in range(len(actual_df["dealer_name"])):
            next_info = dict()
            dn = actual_df["dealer_name"][i]
            tel = actual_df["telephone"][i]
            # Check for the other dealer URLs by dealer name + telephone
            if dn == dealername:
                if tel == phonenumber:
                    next_info['other_urls'] = actual_df["URL"][i]
                    other_url_listings.append(next_info)
                    # Mark the count to 8 random urls, exit if number of urls found are more than 8
                    if len(other_url_listings) > 8:
                        break
                else:
                    print('No other URLs found. So, cannot further fetch details.')
                    next_info['other_urls'] = "None"
                    other_url_listings.append(next_info)

    dealer_df = json_normalize(other_url_listings)
    return dealer_df


# Dealer details extractor, zip file with date is the argument
def listing_dealer_address_extractor(zipname):
    root_path = r"./listings"
    pattern = zipname
    for root, dirs, files in os.walk(root_path):
        for filename in fnmatch.filter(files, pattern):
            print(f'The zip file to be considered is: {os.path.join(root, filename)}')
            zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(root, os.path.splitext(filename)[0]))

# Unzip the date-matched zip file and extract the csv
    root_path_csv = fr"./listings/clean_listings_{Date}"
    pattern_csv = f'clean_listings_{Date}.csv'
    for root, dirs, files in os.walk(root_path_csv):
        for filename_csv in fnmatch.filter(files, pattern_csv):
            base_csv = os.path.join(root, filename_csv)
            print(f'The csv file extracted is: {base_csv}')

# From the actual csv file, extract only the unique URLs per dealer (dealer name + telephone)
    actual_df = pd.read_csv(base_csv)
    unique_values_df = actual_df.drop_duplicates(subset=['dealer_name', 'telephone'],
                                                 keep='last').reset_index(drop=True)
    listings_unique = pd.DataFrame()
    listings_unique = listings_unique.append(unique_values_df)
    # Location and file name consisting of dealer unique urls
    listings_unique.to_csv(fr"./listings/scrape_unique_listings.csv", index=False)
    print('Done with unique listing of dealer urls')

    listings = []
    dealer_address_df = json_normalize(listings)
    with open(fr"./listings/scrape_unique_listings.csv", mode='r') as csv_file:
        unique_df = pd.read_csv(csv_file)
        col_list = ["URL"]
        cols = [c for c in unique_df.columns if
                c in ['URL', 'dealer_name', 'telephone']]

        unique_df = unique_df[cols]
        csv_reader = csv.DictReader(csv_file)
        for i in range(len(unique_df["URL"])):
            info = dict()

            dealer_address_url = unique_df["URL"][i]
            # For each URL, fetch the html
            if pd.notnull(dealer_address_url):
                info['url'] = dealer_address_url
                html = requests.get(dealer_address_url).content
                if html == '':
                    raise CustomError('HTML download error: Empty HTML.')

                # Scrape the dealer details
                soup = BeautifulSoup(html, 'lxml')

                # Dealer Telephone
                for div in soup.findAll('div', attrs={'class': 'vdp-content-wrapper--full'}):
                    next_div = div.findNext('section', attrs={
                        'class': 'fixed-bottom-bar vdp-content-wrapper--full__mobile-contact-section'})
                    dealer_tel_el = next_div.findNext('a')
                    telephone_number = dealer_tel_el['data-phone-number'].replace("tel:+1", "")
                    info['telephone'] = str(re.sub(r'[^\d]', '', telephone_number))

                # Dealer Name
                dealer_name_el = soup.find("h3", {"class": "sds-heading--5 heading seller-name"})
                if dealer_name_el:
                    info['dealer_name'] = dealer_name_el.text.strip()
                else:
                    info['dealer_name'] = 'None'

                # Dealer Website
                dealer_link_el = soup.find("section", {"class": "sds-page-section external-links"})
                if dealer_link_el:
                    link_el = dealer_link_el.find("a")
                    if link_el:
                        url_fetch = urlparse(link_el['href'])
                        actual_url = url_fetch.netloc
                        if actual_url:
                            info['dealer link'] = actual_url
                        else:
                            info['dealer link'] = 'None'

                # Dealer Address
                listings_dealer_address_el = soup.find("div", {"class": "dealer-address"})
                if pd.isnull(listings_dealer_address_el):
                    print('----------Start Fetching with helper URL---------')
                    print(f'The initial call, dealer address is not available for {unique_df["dealer_name"][i]}')
                    # Make a call to listing_similar_dealer_urls_extractor function to fetch other URLs of the dealer
                    other_urls_df = listing_similar_dealer_urls_extractor(base_csv, unique_df["dealer_name"][i], unique_df["telephone"][i])

                    for i in range(1, len(other_urls_df['other_urls'])):
                        other_url = other_urls_df['other_urls'][i]
                        if other_url == 'None':
                            print('No URLs available, Exit')
                            break

                        print(f'Hit the helper URL {other_url} to try fetching the dealer details')
                        if pd.notnull(other_url):
                            # Fetch the html
                            html1 = requests.get(other_url).content
                            if html1 == '':
                                raise CustomError('HTML download error: Empty HTML.')

                            soup = BeautifulSoup(html1, 'lxml')
                            other_listings_dealer_address_el = soup.find("div", {"class": "dealer-address"})
                            # Check if the dealer info is available
                            if pd.isnull(other_listings_dealer_address_el):
                                # Write to the Output file. Update the Status, as info is not available
                                info['status'] = 'Dealer Information Unavailable'
                                info['full address'] = 'None'
                            else:
                                # The next URL of the dealer helped fetch the details, exit after fetching
                                # Update the status
                                info['status'] = 'Fetched With Next URL'
                                print('----------Done Fetching with helper URL---------')
                                # Update the helper URL- which helped us fetch the dealer details
                                info['helper url'] = other_url
                                info['full address'] = other_listings_dealer_address_el.text.strip()
                                break
                else:
                    info['full address'] = listings_dealer_address_el.text.strip()
                    info['status'] = 'All OK'

                listings.append(info)
        else:
            print('No URL, EOF')

    dealer_address_df = json_normalize(listings)
    return dealer_address_df


'''Call main. Provide date as a system argument, as the zip file is picked based on the provided date'''
if __name__ == '__main__':
    ''' On mac run: OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES caffeinate python3 Scrape_Listings.py'''
    if len(sys.argv) >= 1:
        Date = sys.argv[1]
    else:
        Date = eastern_time('%Y-%m-%d', delta=0)  # Yesterday: Date_m1 = eastern_time('%Y-%m-%d', delta=-1)
    print(f'Date set to {Date}')

# Fetch the dealer details
new_dealer_address_df = listing_dealer_address_extractor(f'clean_listings_{Date}.zip')
print('Fetched the dealer details')
listings_dealer_address = pd.DataFrame()
listings_dealer_address = listings_dealer_address.append(new_dealer_address_df)
# Place the output file under dealers folder
listings_dealer_address.to_csv(fr"./dealers/scrape_dealer_details_listing.csv", index=False)
print('Done with dealer address listing')

# Custom error class
class CustomError(Exception):
    pass

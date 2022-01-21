
import pandas as pd
import os
import geocoder
from time import sleep
from random import random

def address_geocode(s):
    # s : address string
    if random()<.5:
        g = geocoder.osm(s)
        if not g.ok: g = geocoder.arcgis(s)
    else:
        g = geocoder.arcgis(s)
        if not g.ok: g = geocoder.osm(s)

    if not g.ok: print(f'Geocoders failed for {s}')
    else: sleep(1)
    # Example Code for Extracting Values:
    # print(g.json)
    # {'full_address': g.address.replace(', United States', ''), 'zipcode': g.postal, 'lat': g.lat, 'lng': g.lng}
    return g

def address_parts(s):
    g = address_geocode(s)
    if not g.ok:
        return {'full_address': 'could not geocode', 'zipcode': '', 'lat': '', 'lng': ''}
    else:
        return {'full_address': g.address.replace(', United States', ''), 'zipcode': g.postal, 'lat': g.lat, 'lng': g.lng}

if __name__ == "__main__":
    Dir = '/Users/sgolara/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Batch03'
    os.chdir(Dir)
    print(f'geocoding df @ {Dir}')
    df = pd.read_csv('./dealers/all_dealer_days.csv')
    geo = df['address'].apply(address_parts).apply(pd.Series)
    expanded_df = df.merge(geo, left_index=True, right_index=True)
    expanded_df.to_csv('/dealers/address_book_geocoded.csv', index=False)
    print(f'saved @ /dealers/address_book.csv')

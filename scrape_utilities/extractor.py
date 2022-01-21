import requests
import json
from bs4 import BeautifulSoup
from json_flatten import flatten
import pandas as pd
import re
from pandas import json_normalize

from scrape_utilities.TorRequest import TorRequest, ip_stats


class CustomError(Exception):
    '''custom error class'''
    pass

def no_match_check(soup):
    noMatch = False
    no_cars_el = soup.find("div", {"class": "no-results"})
    count_el = soup.find('span', {'class': 'total-filter-count'})
    error_el = soup.find('div', {'class': 'error-page-container'})
    if no_cars_el:
        if ('no cars available' in  no_cars_el.text.lower()) or (('no matches found.' in  no_cars_el.text.lower())):
            noMatch = True
    elif count_el:
        if count_el.text.lower().strip().startswith('0 matches'):
            noMatch = True
    elif error_el:
        if 'something went wrong on our end.' in error_el.text.lower():
            noMatch = True
    else:
        noMatch = False
    return noMatch

def too_many_matches_check(soup):
    tooMany = False
    filters_el = ''
    count_el = soup.find('span', {'class': 'total-filter-count'})
    if count_el:
        if '10000+' in count_el.text.lower():
            tooMany = True

    return tooMany


# test_url = 'https://www.cars.com/for-sale/searchresults.action/?mkId=20053&perPage=100&searchSource=GN_REFINEMENT&sort=relevance&stkTypId=28881&rd=5&zc=29223&page=2'
#
test_url = 'https://www.cars.com/shopping/results/?page_size=250&searchSource=GN_REFINEMENT&zc=85281&maximum_distance=all&list_price_min=17996&list_price_max=17999&page=18'
test_url = 'https://www.cars.com/shopping/results/?stock_type=all&makes%5B%5D=audi&models%5B%5D=audi-q5&list_price_max=&maximum_distance=30&zip=85281'
html = requests.get(test_url).content
# html = TorRequest(test_url)[0]

def listing_extractor(html):
    # Check for access denied/empty page:
    if html == '':
        raise CustomError('HTML download error: Empty HTML.')


    soup = BeautifulSoup(html, 'lxml')
    listing_dict = {'vehicle_id': [], 'listing_name': [], 'vehicle_brand': [],
                    'price': [], 'dealer_name': [], 'dealer_state': [],
                    'dealer_rating': [], 'dealer_num_review': [], 'car_color': [], 'listing_url': []}
    '''
    Details of each listing are located in seperate json type files.
    The length of the temp_list file, therefore provides the number of listings on each page
    '''
    # if no match, stop and return 'no match'
    noMatch = no_match_check(soup)
    if noMatch:
        result = 'no matches message'
        return pd.DataFrame(), result
    else:
        # if too many, record a warning and continue
        tooMany = too_many_matches_check(soup)
        if tooMany:
            filters_el = soup.find('input', {'class': 'active-filter-checkbox'})
            if filters_el:
                result = 'too many'


    #num_pages = max([int(p.text) for p in soup.find("ul", {"class": "page-list"}).find_all("a")])

    webpage_json = soup.find("div", {"class": "sds-page-section listings-page", "id": "search-live-content"}).find("script", {"type": "application/ld+json", "id": ""}).string

    json_list = json.loads(webpage_json)['itemListElement']

    webpage_json2 = soup.find('div', {'class': 'sds-page-section listings-page'})['data-site-activity']
    json_list2 = json.loads(webpage_json2)['vehicleArray']
    json_list2[0].keys()

    listings_elements = soup.find_all("div", {"class": "vehicle-card"})
    len(listings_elements)

    listings = []
    for i in range(len(listings_elements)):
        info = dict(json_list[i])
        info.update(json_list2[i])
        l = listings_elements[i]
        info['link'] = l.find("a")['href']

        # Stock type
        stock_el = l.find("p", {"class": "stock-type"})
        if stock_el:
            info['stocktype'] = stock_el.text

        # Mileage
        mileage_el = l.find("div", {"class": "mileage"})
        if mileage_el:
            info['mileage'] = mileage_el.text.replace(' mi.','')

        # # VIN
        # vin_el = l.find('a', attrs={"data-vin": True})
        # if vin_el:
        #     info['vin'] = vin_el['data-vin']


        # Dealer
        dealer_name_el = l.find("div", {"class": "dealer-name"})
        if dealer_name_el:
            info['dealer_name'] = dealer_name_el.text.strip()

        # Dealer Tel
        dealer_tel_el = l.find("div", {"class": "contact-buttons"})
        if dealer_tel_el:
            tel = dealer_tel_el.find('a')
            if tel: info['telephone'] = tel['data-phone-number'].strip()

        # Rating
        rating_el = l.find("span", {"class": "sds-rating__count"})
        if rating_el:
            info['dealer_rating'] = rating_el.text.strip()
        # Num Reviews
        num_reviews_el = l.find("span", {"class": "sds-rating__link sds-button-link"})
        if num_reviews_el:
            info['dealer_reviews'] = num_reviews_el.text.replace('(','').replace(' reviews)','').replace(' review)','').strip()
        # Online Seller
        online_el = l.find("div", {"class": "online-seller"})
        if online_el:
            info['online_seller'] = online_el.text.strip()

        # payment info?
        payment_el = l.find("div", {"class": "price-section price-section-vehicle-card"})
        # payment_el1 = payment_el.find('span', {'class':'primary-price'})
        # if payment_el1:
        #     info['payment1'] = payment_el1.find(text = True, recursive = False)

        if payment_el:
            payment_sub_els = payment_el.find_all("span")
            info['payment_list'] = [x.text.replace('  ','').replace('\n','') for x in payment_sub_els]
        else:
            info['payment_list'] = ['no payment','no payment']

        # any badges?
        badge_el = l.find("div", {"class": "vehicle-badging"})
        if badge_el:
            jj = json.loads(badge_el['data-contents'])
            badges = json_normalize(jj)
            badges_cols = [c for c in badges.columns if all(t not in c for t in ['description', '_tag', 'href_', '_text_lowercase','_icon' ])  and (c != 'price_badge.price_badge')]
            badges = badges[badges_cols]
            badges_cols = [re.sub('price_badge\.|\$bool|_badge|_text', '', c) for c in badges_cols]
            badges_cols = [c.replace('price','deal').replace('hot_car','hot_badge') for c in badges_cols]
            badges.columns = badges_cols
            badges.to_json(orient='records')
            badges_dict = badges.to_dict(orient='records')[0]
            info.update(badges_dict)

            # add other car info
            add = json.loads(badge_el['data-override-payload'])
            add = {k:add[k] for k in add if k not in ['horizontal_position','vertical_position', 'web_page_type_from']}
            info.update(add)

        listings.append(info)

    #Flatten each json and convert to dataframe
    listings_df = json_normalize(listings)

    # Check
    if len(listings_df) > 0:
        result = 'complete'
    elif len(listings_df) == 0:
        result = 'nothing found'
    else:
        pass
    return listings_df, result

def dealer_info_extractor(html):
    # Check for access denied/empty page:
    if html == '':
        raise CustomError('HTML download error: Empty HTML.')
    soup = BeautifulSoup(html, 'lxml')

    # Extract Info
    data = dict()

    address_el = soup.find("div", {"class": "dealer-address"})
    if address_el:
        data['address'] = address_el.text

    dealer_link_el = soup.find("section", {"class": "sds-page-section external-links"})
    if dealer_link_el:
        link_el = dealer_link_el.find("a")
        if link_el:
            m = re.search(r'http[s]*://www\..*[/^]', link_el['href'])
            if m:
                data['dealer_link'] = m[0]

    #Flatten dict and convert to dataframe
    data = json_normalize(data)

    return data



def feature_extractor(html, flatten=False):
    soup = BeautifulSoup(html, 'lxml')

    'is the car sold?'
    not_available_msg = soup.find("h1", {"class": "cui-heading-2 sold-vehicle__main-title"}) #sold vehicle indicator
    if not_available_msg:
        # print(not_available_msg.text)
        not_available_msg
        output = {'notavailable':'sold'}
    else:
        detailed_features = []
        detailed_features_element = soup.find("ul", {"class": "vdp-details-basics__features-list"})
        if detailed_features_element:
            detailed_features = detailed_features_element.text.split("\n")

        basic_features = {}
        basic_features_element = soup.find("ul", {"class": "vdp-details-basics__list"})
        if basic_features_element:
            basic_features = list(filter(None,basic_features_element.text.split("\n")))
            #Quick Clean-up
            basic_features = [s.replace('  Based on EPA mileage ratings. Use for comparison purposes only. Actual mileage will vary depending on driving conditions, driving habits, vehicle maintenance, and other factors. ','') for s in basic_features]
            basic_features = {f.split(': ')[0]:f.split(': ')[1] for f in basic_features}

        '''
        The data structure for the detailed listing is slightly complicated.
        There are 5 categories for detailed feature:
        1. Convenience
        2. Entertainment
        3. Safety
        4. Exterior
        5. Seating
    
        I therefore store the result from each listing as a dictionary, with
        the key being one of the broad feature categories as above, and the 
        values being each feature within the category
    
        Therefore, I initialize a result dictionary at the beginning of each iteration 
        of the loop, to store the contents from features
    
        The detailed features for each listing are nested within a parent class called:
        details-features-list--normalized-features, which is extracted from the URL
    
        -->It contains several children classes, called "cui-heading-2", which store
        the five broad feature categories defined above. This becomes the 'Key'
    
        --> Each childrent class also has its own childrent class, called 
        "details-feature-list__item", which is the actual feature. These become
        the 'value'. Since there are multiple values within each 'Key', we iterate over
        all of them
        '''

        top_features = {}
        for group in soup.find_all("div", {"class": "details-feature-list--normalized-features"}):
            top_features[group.find("h2", {"class": "cui-heading-2"}).text] = [itm.text for itm in group.find_all("li", {
                "class": "details-feature-list__item"})]

        features = {}
        features['basic'] = basic_features
        features['det'] = detailed_features
        features['top'] = top_features
        output = features
        if flatten:
            # Flatten with numbered columns
            flat1 = pd.DataFrame(flatten(features), index=[0])

            # Flatten with features columns
            features2 = {}
            features2['basic'] = basic_features
            features2['det'] = {k:'1' for k in detailed_features}
            features2['top'] = {k:{v:'1' for v in top_features[k] } for k in top_features }
            flat2 = pd.DataFrame(flatten(features2), index=[0])
            output = features, flat1, flat2

    return output

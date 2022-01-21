from uszipcode import SearchEngine
search = SearchEngine(simple_zipcode=True) # set simple_zipcode=False to use rich info database
zipcode = search.by_zipcode("33196")
zipcode





zipcode.values() # to list
[u'10001', u'Standard', u'New York', u'New York, NY', [u'New York'], u'New York County', u'NY', 40.75, -73.99, u'Eastern', 0.9090909090909091, [u'718', u'917', u'347', u'646'], 21102, 33959.0, 0.62, 0.0, 12476, 11031, 650200, 81671, -74.008621, -73.984076, 40.759731, 40.743451]

zipcode.to_dict() # to dict
{'housing_units': 12476, 'post_office_city': u'New York, NY', 'bounds_east': -73.984076, 'county': u'New York County', 'population_density': 33959.0, 'radius_in_miles': 0.9090909090909091, 'timezone': u'Eastern', 'lng': -73.99, 'common_city_list': [u'New York'], 'zipcode_type': u'Standard', 'zipcode': u'10001', 'state': u'NY', 'major_city': u'New York', 'population': 21102, 'bounds_west': -74.008621, 'land_area_in_sqmi': 0.62, 'lat': 40.75, 'median_household_income': 81671, 'occupied_housing_units': 11031, 'bounds_north': 40.759731, 'bounds_south': 40.743451, 'area_code_list': [u'718', u'917', u'347', u'646'], 'median_home_value': 650200, 'water_area_in_sqmi': 0.0}

zipcode.to_json() # to json
{
    "zipcode": "10001",
    "zipcode_type": "Standard",
    "major_city": "New York",
    "post_office_city": "New York, NY",
    "common_city_list": [
        "New York"
    ],
    "county": "New York County",
    "state": "NY",
    "lat": 40.75,
    "lng": -73.99,
    "timezone": "Eastern",
    "radius_in_miles": 0.9090909090909091,
    "area_code_list": [
        "718",
        "917",
        "347",
        "646"
    ],
    "population": 21102,
    "population_density": 33959.0,
    "land_area_in_sqmi": 0.62,
    "water_area_in_sqmi": 0.0,
    "housing_units": 12476,
    "occupied_housing_units": 11031,
    "median_home_value": 650200,
    "median_household_income": 81671,
    "bounds_west": -74.008621,
    "bounds_east": -73.984076,
    "bounds_north": 40.759731,
    "bounds_south": 40.743451
}
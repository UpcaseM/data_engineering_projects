#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 10:10:00 2020

@author: UpcaseM

"""

import os
#import sys
import json
#import pickle
import requests
import numpy as np
import pandas as pd
import configparser
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def time_list(start_date, end_date):
    '''
    Create a list of date string. The intervel is 30 days.

    Parameters
    ----------
    start_date : string
        eg. 2004-01-01
    end_date : string
        eg. 2020-01-01

    Returns
    -------
    lst : list
        time interval string
    '''
    lst = []
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    next_date = start_date
    while next_date <= end_date:
        lst.append((str(next_date).split(' ')[0].replace('-', ''),
                    str(next_date + datetime.timedelta(days=30)) \
                        .split(' ')[0].replace('-', '')))
        next_date = next_date + datetime.timedelta(days=30)
    return lst


def weather_scraper(df_location, 
                    driver_path, 
                    start_date, 
                    end_date,
                    api_key,
                    data_dir):
    '''
    Weather data scraper. Download histical data from www.wunderground.com
    Parameters
    ----------
    df_location : pandas dataframe
        dataframe contains two columns: latit_s, longi_s.  The geo location.
    driver_path : string
        path of the chrome driver
        Download the correct driver for the chrome you installed.
        https://www.howtogeek.com/299243/which-version-of-chrome-do-i-have/
        https://chromedriver.chromium.org/downloads
    start_date : string
        from which date we scrape, eg. 2004-01-01
    end_date : string
        the last date of histical data we want to get, eg. 2020-01-01
    api_key : string
        api key for wunderground API
        The api key is generated automaticlly when you visit the website
        www.wunderground.com Use chrome dev tool to find out the api key
    data_dir: string
        data dir of the project
    Returns
    -------
    dict_station : dictionary
        dict to store all weather data.
    df_city_station: dataframe
        add a station column for all cities
    '''
    chrome_options = Options()  
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(driver_path, options=chrome_options)
    
    lst_station = []
    # Look up closest station based on city and state.
    print('Scraping station data...')
    api_key=str(api_key)
    for i, row in df_location.iterrows():
        url = 'https://api.weather.com/v3/location/search' \
            + '?apiKey={}'.format(api_key) \
            + '&language=en-US&locationType=city%2Cairport%2CpostCode%2Cpws&format=json' \
            + '&query={}'.format(str(row[0])+','+str(row[1]))
        
        driver.get(url) 
        data = driver.find_element_by_tag_name('pre').text
        station = json.loads(data)['location']['icaoCode'][0]
        lst_station.append(station)
    
    df_location['station'] = lst_station
    # Fill na with the station of KPHX
    df_location = df_location.replace(r'^\s*$', np.nan, regex=True)
    df_location.fillna('KPHX', inplace = True)
    # Save df_city to yelp-dataset
    df_location.to_csv(data_dir + '/yelp-dataset/geo_location.csv')
    print('Station data is ready!')
    # The website only allow to get data of 30 days.
    lst_time = time_list(start_date,end_date)
    
    # Scrape all histical data based on station.
    # Create a dictionary to store all json data
    set_station = set(df_location.loc[:,'station'].unique())
    print(set_station)
    dict_station = dict(zip(set_station, 
                           [[] for i in range(len(set_station))]))
    print(f'Scraping weather data from {len(set_station)} stations...')
    for s in dict_station:
        for inter in lst_time:
            print(f'scraping {s} for {inter}')
            url = 'https://api.weather.com/v1/location' \
                + '/{}:9:US/observations/historical.json'.format(s) \
                + '?apiKey={}'.format(api_key) \
                + '&units=e' \
                + '&startDate={}'.format(inter[0]) \
                + '&endDate={}'.format(inter[1])
            
            driver.get(url)
            data = driver.find_element_by_tag_name('pre').text
            data = json.loads(data)
            if data['metadata']['status_code'] == 200:
                dict_station[s].append(data['observations'])
        parse_data(s, dict_station[s], data_dir + '/weather-data')
    return dict_station, df_location

def parse_data(station_name, weather_data, output_path):
    '''
    Parse weather data and save data in csv files for each station

    Parameters
    ----------
    station_name : string
        name of the station
    weather_data : list
        weather data
    output_path : string
        dir to store the csv files.

    Returns
    -------
    None.

    '''
    for i, data in enumerate(weather_data):
        
        df_weather = pd.DataFrame(data)
        df_weather['valid_time_gmt'] = pd.to_datetime(df_weather.loc[:,'valid_time_gmt'],
                                                      unit = 's')
        df_weather['date'] = df_weather.loc[:,'valid_time_gmt'].dt.date
        df_weather['hour'] = df_weather.loc[:, 'valid_time_gmt'].dt.hour
        df_weather.dropna(axis=1, inplace = True)
        df_weather['key'] = df_weather.loc[:,'key'].map(lambda x: station_name)
        df_weather = df_weather.loc[:, ['key', 
                                        'obs_name', 
                                        'date',
                                        'hour',
                                        'temp',
                                        'dewPt',
                                        'rh',
                                        'pressure',
                                        'precip_hrly',
                                        'wx_phrase',
                                        'uv_desc',
                                        'feels_like']]
        df_weather.columns = ['station', 
                              'station_name',
                              'date',
                              'hour',
                              'temp',
                              'dew_point_f',
                              'humidity%',
                              'pressure',
                              'precip_hrly',
                              'condition',
                              'uv_level',
                              'feels_like']
        if i == 0:
            df = df_weather
        else:
            df = pd.concat([df, df_weather], ignore_index = True)
    df.to_csv(output_path + '/' + station_name + '.csv')
    print(f'Histical weather data is saved in {output_path}')
        
def create_yelp_category(api_key, ouput_path):
    '''
    Extract yelp business category data through yelp API

    Parameters
    ----------
    api_key : string
        yelp api key
    ouput_path : string
        dir to store the csv file.
    Returns
    -------
    csv file

    '''
    api_key = str(api_key)
    headers = {'Authorization': 'Bearer %s' % api_key}
    r = requests.get('https://api.yelp.com/v3/categories', 
                     headers=headers)
    df = pd.DataFrame(r.json()['categories'])
    df = df.explode('parent_aliases').reset_index(drop = True)
    # Find parents for all subcategories
    
    id_name_dict = dict(zip(df.alias, df.title))
    parent_dict = dict(zip(df.alias, df.parent_aliases))
    
    def find_parent(x):
        '''
        Function to find all parent category
        '''
        value = parent_dict.get(x, None)
        if value is None:
            return ''
        else:
            # Incase there is a id without name.
            if id_name_dict.get(value, None) is None:
                return '' + find_parent(value)
    
            return str(id_name_dict.get(value)) +', '+ find_parent(value)
        
    # Create a column with category and all subcategories.
    df['all_cat'] = df['alias'] \
        .map(lambda x: find_parent(x)) \
        .str.rstrip(', ') \
        .map(lambda x: x.split(', '))
    df.apply(lambda x: x['all_cat'].insert(0, x['title']), axis = 1)
    df['all_cat'] = df.loc[:,'all_cat'].map(lambda x: list(reversed([x[0]])) if x[-1] == '' else list(reversed(x)))
    df_lst = df.loc[:,'all_cat'].to_list()
    # Only keep the category and the first subcategory
    df_category = pd.DataFrame(df_lst).iloc[:,[0,1]]
    df_category.columns = ['category','subcategories']
    df_category.drop_duplicates(inplace = True)
    df_category = df_category.reset_index(drop = True)
    df_category.index.name = 'index'
    df_category.to_csv(ouput_path + '/yelp_category.csv')
    print(f'yelp_category.csv is created in {ouput_path}')

# Create dir tree 
# https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
from pathlib import Path
from itertools import islice

space =  '    '
branch = '│   '
tee =    '├── '
last =   '└── '
def tree(dir_path: Path, level: int=-1, limit_to_directories: bool=False,
         length_limit: int=1000):
    """Given a directory Path object print a visual tree structure"""
    dir_path = Path(dir_path) # accept string coerceable to Path
    files = 0
    directories = 0
    def inner(dir_path: Path, prefix: str='', level=-1):
        nonlocal files, directories
        if not level: 
            return # 0, stop iterating
        if limit_to_directories:
            contents = [d for d in dir_path.iterdir() if d.is_dir()]
        else: 
            contents = list(dir_path.iterdir())
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, path in zip(pointers, contents):
            if path.is_dir():
                yield prefix + pointer + path.name
                directories += 1
                extension = branch if pointer == tee else space 
                yield from inner(path, prefix=prefix+extension, level=level-1)
            elif not limit_to_directories:
                yield prefix + pointer + path.name
                files += 1
    print(dir_path.name)
    iterator = inner(dir_path, level=level)
    for line in islice(iterator, length_limit):
        print(line)
    if next(iterator, None):
        print(f'... length_limit, {length_limit}, reached, counted:')
    print(f'\n{directories} directories' + (f', {files} files' if files else ''))

# =============================================================================
# Runing webscraper
# =============================================================================
if __name__ == '__main__':
# =============================================================================
     path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
     #Create dir tree
     #tree(path, level=3)
     config = configparser.ConfigParser()
     config.read(path + '/resource/project.cfg')
     df_location = pd.read_csv(path + '/data/yelp-dataset/geo_location.csv') \
         .loc[:, ['latit_s', 'longi_s']]
     _,df_location = (weather_scraper(df_location, 
                                      path + '/resource/chromedriver', 
                                      '2004-01-01', 
                                      '2020-02-01',
                                      config['WD']['API_KEY'],
                                      path + '/data'))


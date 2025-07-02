import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import sys



# Comment out line 12 when loading cloud db (future)
db = ''
db = db + 'LOCAL_'

# Set variables
DB_NAME = os.environ[f'{db}DB_NAME']
DB_USER = os.environ[f'{db}DB_USER']
DB_HOST = os.environ[f'{db}DB_HOST']
DB_PASSWORD = os.environ[f'{db}DB_PASSWORD']
NOAA_TOKEN = os.environ['NOAA_TOKEN']

header = {'token': NOAA_TOKEN}
base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations'
# base_url = "https://www.ncei.noaa.gov/access/services/data/v1" alternative possiblity - have not checked
dataset_id = '?datasetid=GHCND'
limit = '&limit=1000'




def db_connect():
    connection_string = f'dbname={DB_NAME} user={DB_USER} host={DB_HOST} password={DB_PASSWORD}'

    try:
        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        print('Database connection successful')
    
    except:
        sys.exit('Unable to connect to the database')

    cursor = connection.cursor(cursor_factory=DictCursor)
    return cursor



def load_db(json_response):
    for result in json_response['results']:
        
        try:
            insert_sql = """INSERT INTO weather.weather_stations
                (station_id, name, latitude, longitude, elevation, elevation_unit, country_code, region, min_date, max_date, data_coverage)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (station_id) DO UPDATE SET
                name=%s,latitude=%s,longitude=%s,elevation=%s,elevation_unit=%s,min_date=%s,max_date=%s,data_coverage=%s"""
            
            cursor.execute(insert_sql, (result['id'], result['name'], result['latitude'], result['longitude'], result.get('elevation'),\
                            result.get('elevationUnit'), None, None, result['mindate'], result['maxdate'], result['datacoverage'],\
                            result['name'], result['latitude'], result['longitude'], result.get('elevation'), result.get('elevationUnit'),\
                            result['mindate'], result['maxdate'], result['datacoverage']))

        except:
            cursor.close()
            sys.exit('Unable to load database')
    return



def load_weather_stations(entry_number=1, attempts=1):
    offset = f'&offset={entry_number}'
    url = base_url + dataset_id + limit + offset
    retries = 4

    # Make request to NOAA API
    try:
        response = requests.get(url, headers=header)
        status_code = response.status_code
        
        if status_code == 200:
            json_response = response.json()
            num_results = json_response['metadata']['resultset']['count']
            
            # Need to reset attempts here for next API call
            attempts = 1

            # This is a recursive function - only want to see number of results at the beginning
            if entry_number == 1:
                print(f"Number of stations: {num_results}")

            
            load_db(json_response)
        
            entry_number += 1000
            # using min here for the last call which is typically less than 1000 entries
            print(f'Loaded {min(entry_number - 1, num_results)} entries')     
            
            if (entry_number <= num_results):
                load_weather_stations(entry_number, attempts)
        
        # API is a bit glitchy at times, usually just need to re-request
        elif status_code == 503:
            print(f'Status code: {status_code}')

            if attempts < retries:
                attempts += 1
                print(f'Attempt number {attempts} initiating...')
                load_weather_stations(entry_number, attempts)
            
            else:
                print(f'Failed {retries} attempts')
        
        else:
            print(f'Exiting with status code: {status_code}')
                
    except:
        print('NOAA request failed')
    return




if __name__ == "__main__":
    # Connect to database
    cursor = db_connect()

    # Loads weather station data (1000 at a time) into database
    load_weather_stations()

    cursor.close()

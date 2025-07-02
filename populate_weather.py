import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import time
import sys
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import folium
from datetime import date, datetime
from loguru import logger





# Set up logging
def serializer(record):
    subset = {
        "time": record["time"].strftime("%Y-%m-%d %H:%M:%S"),
        "level": record["level"].name,
        "line": record["line"],
        "message": record["message"],
        "context": record["extra"]
    }
    return json.dumps(subset)


def add_serialization(record):
    record["extra"]["json_output"] = serializer(record)


logger.remove()
logger = logger.patch(add_serialization)

logger.add("populate_weather_log.json", format="{extra[json_output]}")
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <yellow>{level}</yellow> | <cyan>line: {line}</cyan> | <cyan>{message}</cyan>")

script_run_datetime = time.time()
script_logger = logger.bind(script_run_datetime=script_run_datetime)





# Comment out line 50 when loading cloud db
db = ''
db = db + 'LOCAL_'

# Set variables
DB_NAME = os.environ[f'{db}DB_NAME']
DB_USER = os.environ[f'{db}DB_USER']
DB_HOST = os.environ[f'{db}DB_HOST']
DB_PASSWORD = os.environ[f'{db}DB_PASSWORD']
NOAA_TOKEN = os.environ['NOAA_TOKEN']
header = {'token': NOAA_TOKEN}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND"
datatype = "&datatypeid=TMIN,TMAX,PRCP,SNOW,SNWD"
station_id_pre = "&stationid="
start_date_pre = "&startdate="
end_date_pre = "&enddate="
units = "&units=standard"
limit = "&limit=1000"
offset_pre = "&offset="




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



def load_api_limit_list():
    if os.path.exists('api_calls.json'):
        
        try:
            with open('api_calls.json', 'r') as file_in:
                DAILY_RATE_LIMIT = json.load(file_in)
            
            return DAILY_RATE_LIMIT
        
        except:
            return []
    
    else:
        return []



def save_api_limit_list():
    with open('api_calls.json', 'w') as file_out:
        json.dump(DAILY_RATE_LIMIT, file_out)
    
    return



def load_data(results):
            
    for result in results:
        try:
            insert_sql = "INSERT INTO weather.weather_usa_97 (station_id, date, datatype, value, attributes) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (station_id, date, datatype) DO UPDATE SET value = %s, attributes = %s"
            cursor.execute(insert_sql, (result['station'], result['date'], result['datatype'], result['value'], result['attributes'], result['value'], result['attributes']))
        
        except:
            save_api_limit_list()
            cursor.close()
            script_logger.error('Unable to load database', station=result['station'])
            sys.exit()
    
    return



def rate_limit_check(url):
    global SECONDS_RATE_LIMIT
    global DAILY_RATE_LIMIT

    
    # SECONDS RATE LIMIT CHECK
    # check max of 5 api calls per second
    now = time.time()

    if len(SECONDS_RATE_LIMIT) < 4:
        SECONDS_RATE_LIMIT.append(now)
    
    else:
        time_diff = now - SECONDS_RATE_LIMIT[0]
        
        if time_diff < 1.1:
            sleep_time = 1.1 - time_diff
            time.sleep(sleep_time)
            SECONDS_RATE_LIMIT.append(now + sleep_time)
        
        else:
            SECONDS_RATE_LIMIT.append(now)
        
        SECONDS_RATE_LIMIT = SECONDS_RATE_LIMIT[1:]
    
    
    # DAILY RATE LIMIT CHECK
    # check max of 10,000 api calls per day
    now = time.time()

    if len(DAILY_RATE_LIMIT) < 9999:
        DAILY_RATE_LIMIT.append(now)
    
    else:
        time_diff = now - DAILY_RATE_LIMIT[0]

        if time_diff < 86460.0:
            print(f'10,000 calls per day limit reached in {round(time_diff/3600, 2)} hours')

            save_api_limit_list()
            cursor.close()
            script_logger.error('Daily API Limit Exceeded', url=url)
            sys.exit()
        
        else:
            DAILY_RATE_LIMIT.append(now)
        
        DAILY_RATE_LIMIT = DAILY_RATE_LIMIT[1:]
    
    return
        


def get_data(url_pre, offset=1, attempts=1):
    url = url_pre + str(offset)
    
    rate_limit_check(url)
    try:
        response = requests.get(url, headers=header, timeout=120)
        status_code = response.status_code
    
    except:
        script_logger.error('Request failed', url=url)
        return
    

    if status_code == 200:
        # Need to reset attempts here for next API call
        attempts = 1

        json_results = response.json()

        try:
            results = json_results['results']
        
        except KeyError:
            script_logger.warning('No results', url=url)
            return
        
        load_data(results)

        offset += 1000
        if (offset <=  json_results['metadata']['resultset']['count']):
            get_data(url_pre, offset, attempts)
            
    # API is a bit glitchy at times, usually just need to re-request
    elif status_code >= 500 and status_code < 600:

        if attempts < retries:
            attempts += 1
            get_data(url_pre, offset, attempts)
        
        else:
            script_logger.error('Exceeded retries', status_code=status_code, url=url)
    
    else:
        save_api_limit_list()
        cursor.close()
        script_logger.error(f'Unknown error: {status_code}', status_code=status_code, url=url)
        sys.exit()
    
    return
    


def api_call_generator(station_id, mindate, maxdate):
    start_yr, end_yr = mindate[:4], maxdate[:4]
    num_years = int(end_yr) - int(start_yr) +1


    for year in range(num_years):

        if num_years == 1:
            url_pre = base_url + datatype + station_id_pre + station_id + start_date_pre + mindate + end_date_pre + maxdate + units + limit + offset_pre
            get_data(url_pre)

        elif year == 0:
            url_pre = base_url + datatype + station_id_pre + station_id + start_date_pre + mindate + end_date_pre + start_yr + "-12-31" + units + limit + offset_pre
            get_data(url_pre)

        elif year == num_years - 1:
            url_pre = base_url + datatype + station_id_pre + station_id + start_date_pre + end_yr + "-01-01" + end_date_pre + maxdate + units + limit + offset_pre
            get_data(url_pre)

        else:
            url_pre = base_url + datatype + station_id_pre + station_id + start_date_pre + str(int(start_yr) + year) + "-01-01" + end_date_pre + str(int(start_yr) + year) + "-12-31" + units + limit + offset_pre
            get_data(url_pre)
    
    return



# clustering algorithm
def cluster_stations(df, radius):
    coords = df[['latitude', 'longitude']].to_numpy()
    kms_per_radian = 6371.0088
    epsilon = radius / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    return clusters



# choose point in cluster with highest coverage
def get_highest_coverage_station(clusters, stations):
    points = pd.DataFrame()
    for cluster in clusters:
        lats, lons = zip(*cluster)
        cluster_df = pd.DataFrame({'lat':lats, 'lon':lons})
        cluster_df = cluster_df.apply(lambda row: stations[(stations['latitude']==row['lat']) & (stations['longitude']==row['lon'])].iloc[0], axis=1)
        chosen = cluster_df[cluster_df.datacoverage == cluster_df.datacoverage.max()]
        points = pd.concat([points, chosen.head(1)], ignore_index=True, sort=False)
    return points


def create_global_map(station_results, outfile):
    f = folium.Figure()
    m = folium.Map(location=(30, 10)\
                , zoom_start=3\
                , min_zoom=3\
                , tiles="cartodb positron"\
                , max_bounds=True).add_to(f)
    
    for row in station_results:
        folium.CircleMarker([row[1], row[2]], radius=1, color='purple', fill=True, fill_color='purple').add_to(m)
    
    if not os.path.exists('weather_maps'):
        os.system('mkdir weather_maps')
    
    m.save(f'weather_maps/{outfile}.html')
    print(f'Map saved at: weather_maps/{outfile}.html')

    return



def create_usa_map(station_results, outfile):
    f = folium.Figure()
    m = folium.Map(location=(40, -98.5)\
                , zoom_start=4\
                , min_zoom=3\
                , tiles="cartodb positron"\
                , max_bounds=True).add_to(f)
    
    for row in station_results:
        folium.CircleMarker([row[1], row[2]], radius=1, color='purple', fill=True, fill_color='purple').add_to(m)
    
    if not os.path.exists('weather_maps'):
        os.system('mkdir weather_maps')
    
    m.save(f'weather_maps/{outfile}.html')
    print(f'Map saved at: weather_maps/{outfile}.html')

    return



def get_log_rerun_stations():
    try:
        with open('populate_weather_log.json', 'r') as file:
            log_entries = [json.loads(line) for line in file]
            
        log_df = pd.json_normalize(log_entries).rename(columns={'context.url':'url',\
                                                                'context.status_code':'status_code',\
                                                                'context.attempts':'attempts',\
                                                                'context.key_error':'key_error',\
                                                                'context.station':'station',\
                                                                'context.script_run_datetime':'script_run_datetime'})
        log_df.drop_duplicates(inplace=True)

        if 'station' in log_df.columns:
            log_df['station'].fillna(log_df['url'].str[108:125])
        else:
            log_df['station'] = log_df['url'].str[108:125]
        
        return log_df.loc[log_df.level == 'ERROR', 'station'].unique().to_list()
    
    except:
        return []



def populate_weather(filtered_stations, mindate, maxdate, single_station_load=True, rerun_fails=False): 
    if rerun_fails:
        reruns = get_log_rerun_stations()
        
    else:
        reruns = []

    query = "SELECT DISTINCT station_id FROM weather.weather_usa_97"
    cursor.execute(query)
    stations_loaded = cursor.fetchall()

    stations_loaded = [result[0] for result in stations_loaded]
    print(f'Number of stations to load: {len(filtered_stations) - len(stations_loaded) + len(reruns)}')

    for station_result in filtered_stations:
        station_id = station_result[0]
        
        if station_id in stations_loaded and station_id not in reruns:
            continue
        
        mindate = max(station_result[3], mindate)
        maxdate = min(station_result[4], maxdate)
        print(f'\nRetrieving data for station: {station_id}')
        print(f'mindate: {mindate}, maxdate: {maxdate}')

        api_call_generator(station_id, str(mindate), str(maxdate))
        
        if single_station_load:
            break
    
    return



def filter_stations(create_station_html=True):
    query = """
            SELECT station_id, latitude, longitude, min_date, max_date FROM weather.weather_stations
            WHERE country_code = 'US'
            AND max_date >= ('2025-05-13')::date
            AND min_date <= ('1950-01-01')::date
            AND data_coverage >= 0.97"""
    # CURRENT_DATE - INTERVAL '30 days'
    
    cursor.execute(query)
    station_results = cursor.fetchall()

    if create_station_html:
        # create_global_map(station_results, 'weather_stations_usa_97')
        create_usa_map(station_results, 'weather_stations_usa_97')


    # # use clustering algorithm and choose station with highest coverage
    # radii = [5, 25, 50, 75, 100, 150, 200, 300, 400, 500]
    # radii = radii[:resolution + 1]

    # for radius in radii:
    #     clusters = cluster_stations(df, radius)
    #     df = get_highest_coverage_station(clusters, df)
    
    # filtered_stations = df['id'].to_list()
    # station_results = [x for x in station_results if x[0] in filtered_stations]
    
    return station_results




if __name__ == "__main__":
    # Connect to database
    cursor = db_connect()

    
    '''
    set variables
    '''
    # creates empty list for seconds
    SECONDS_RATE_LIMIT = []
    
    # creates either an empty list for daily or load in existing from api_calls.json
    DAILY_RATE_LIMIT = load_api_limit_list()
    
    # RETRIES is number of api calls when there is a 503 error before skipping
    retries = 4

    # resolution is 0 - 9 and refers to the index in this list
    # [5, 25, 50, 75, 100, 150, 200, 300, 400, 500]
    # the list values are the radius in kilometers for the clusters
    # this is only used when reducing stations geographically with clustering algorithm in filter_stations()
    resolution = 5

    min_date = datetime.strptime('1950-01-01', '%Y-%m-%d').date()
    max_date = date.today()

    
    filtered_stations = filter_stations(create_station_html=False)

    populate_weather(filtered_stations, min_date, max_date, single_station_load=False)

    save_api_limit_list()
    cursor.close()


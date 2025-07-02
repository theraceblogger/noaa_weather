import os
import psycopg2
from psycopg2.extras import DictCursor
import reverse_geocoder
import sys



# Comment out line 12 when loading cloud db
db = ''
db = db + 'LOCAL_'

# Set variables
DB_NAME = os.environ[f'{db}DB_NAME']
DB_USER = os.environ[f'{db}DB_USER']
DB_HOST = os.environ[f'{db}DB_HOST']
DB_PASSWORD = os.environ[f'{db}DB_PASSWORD']




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






iso_dict = {
    # Asia
    'southern_asia':['AF', 'BD', 'BT', 'IN', 'IR', 'LK', 'MV', 'NP', 'PK'],
    'western_asia':['AE', 'AM', 'AZ', 'BH', 'CY', 'GE', 'IQ', 'IL', 'JO', 'KW', 'LB', 'OM', 'PS', 'QA', 'SA', 'SY', 'TR', 'YE'],
    'southeastern_asia':['BN', 'ID', 'KH', 'LA', 'MM', 'MY', 'PH', 'SG', 'TH', 'VN', 'TL'],
    'eastern_asia':['CN', 'HK', 'JP', 'KR', 'MO', 'MN', 'KP', 'TW'],
    'central_asia':['KZ', 'KG', 'TJ', 'TM', 'UZ'],
    # Africa
    'middle_africa':['AO', 'CF', 'CM', 'CG', 'GA', 'GQ', 'ST', 'TD', 'CD'],
    'eastern_africa':['BI', 'KM', 'DJ', 'ER', 'ET', 'KE', 'MG', 'MZ', 'MU', 'MW', 'RE', 'RW', 'SO', 'SC', 'TZ', 'UG', 'ZM', 'ZW',\
        'YT', 'TF', 'IO'],
    'western_africa':['BJ', 'BF', 'CI', 'CV', 'GH', 'GN', 'GM', 'GW', 'LR', 'ML', 'MR', 'NE', 'NG', 'SN', 'SH', 'SL', 'TG'],
    'southern_africa':['BW', 'LS', 'NA', 'SZ', 'ZA'],
    'northern_africa':['DZ', 'EG', 'EH', 'LY', 'MA', 'SD', 'TN', 'SS'],
    # Americas
    'caribbean':['AI', 'CW', 'AG', 'BS', 'BB', 'CU', 'KY', 'DM', 'DO', 'GP', 'GD', 'HT', 'JM', 'KN', 'LC', 'MS', 'MQ', 'PR', 'TC',\
        'TT', 'VC', 'VG', 'VI', 'AW', 'BQ', 'BL', 'CW', 'MF', 'SX'],
    'south_america':['AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'FK', 'GF', 'GY', 'PE', 'PY', 'SR', 'UY', 'VE', 'BV', 'GS'],
    'central_america':['BZ', 'CR', 'GT', 'HN', 'MX', 'NI', 'PA', 'SV'],
    'northern_america':['BM', 'CA', 'GL', 'PM', 'US'],
    # Europe
    'southern_europe':['AL', 'AD', 'PT', 'BA', 'ES', 'GI', 'GR', 'HR', 'IT', 'MK', 'MT', 'PT', 'SM', 'RS', 'SI', 'VA', 'ME'],
    'western_europe':['AT', 'BE', 'CH', 'DE', 'FR', 'LI', 'LU', 'MC', 'NL'],
    'eastern_europe':['BG', 'BY', 'CZ', 'HU', 'MD', 'PL', 'RO', 'RU', 'SK', 'UA'],
    'northern_europe':['DK', 'EE', 'FI', 'FO', 'GB', 'IE', 'IS', 'LT', 'LV', 'IM', 'NO', 'SE', 'SJ', 'AX', 'GG', 'JE'],
    'russian_federation':['RU'],
    # Oceania
    'polynesia':['AS', 'CK', 'NU', 'PN', 'PF', 'TK', 'TO', 'TV', 'WF', 'WS'],
    'australia_new_zealand':['AU', 'NZ', 'NF', 'CC', 'CX', 'HM'],
    'melanesia':['FJ', 'NC', 'PG', 'SB', 'VU'],
    'micronesia':['FM', 'GU', 'KI', 'MH', 'NR', 'PW', 'MP', 'UM'],
    # Antarctica
    'antarctica':['AQ']}






def get_stations():
    query = """
            SELECT station_id, latitude, longitude FROM weather.weather_stations
            WHERE country_code IS NULL"""
    
    cursor.execute(query)
    results = cursor.fetchall()

    print(f'Number of results: {len(results)}')
    return results



def update_cc_region_stations(stations):
    
    for station in stations:

        location = reverse_geocoder.search((station[1], station[2]))
        country_code = location[0]['cc']

        for key, value in iso_dict.items():
            if country_code in value:
                region = key
                break
        else:
            print(f'Could not determine region for {country_code}')
            region = None

        insert_sql = "UPDATE weather.weather_stations SET country_code = %s, region = %s WHERE station_id = %s"
        cursor.execute(insert_sql, (country_code, region, station[0]))

    print('Country codes and regions update complete')
    
    return





if __name__ == "__main__":
    # Connect to database
    cursor = db_connect()

    # get stations with coordinates
    stations = get_stations()

    update_cc_region_stations(stations)

    cursor.close()

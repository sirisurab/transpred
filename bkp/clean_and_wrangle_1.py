
# coding: utf-8

# In[1]:


import pandas as pd
import dask.dataframe as dd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
import matplotlib.pyplot as plt
import re
root = 'data/'


# In[2]:


from geopandas import GeoDataFrame
from shapely.geometry import Point
from shapely.geometry import LineString

# # LOAD STATION DATA

# Source : NYC MTA (Subway Stations Data)
#
# Description : Description of all the subway stations in NYC. Useful fields are 'STOP_NAME' (station name) and 'GTFS Latitude' and 'GTFS Longitude' (geographic coordinates of the station)
#
# Processing : This dataset has been processed by the stations.ipynb python notebook and saved to Stations_geomerged.geojson
#
# Following data issues have been addressed :
#
# 1. There is no unique identifier that represents stations across the NYC MTA database : The 'STATION' column of the MTA turnstile dataset is the only identifier for the station in that set. The contents of this column differed significantly from the 'STOP_NAME' column of the stations dataset. For example, the station named 'TIMES SQ-42 ST' in one set was represented as 'Times Sq - 42 St' in the other. Although, issues like this were easy to fix, there were a fair number of cases where a station named 'Astoria - Ditmars Blvd' did not have any obvious match in the other data set. A possible cause for cases like this is the use of different station names for the same station ('Astoria - Ditmars Blvd' station was earlier known as 'Second Avenue'). Cases like this are hard, if not impossible to match. A python string-matching library called 'fuzzy-wuzzy' was used to find the best matches using 3 Levenshtein closeness ratios (normal ratio, partial ratio and token sort ratio). The match was accepted only if one of the three matching methods returned a ratio of 88% or higher. The matching station names from the trunstile dataset were added to the 'STATION' column of the stations dataset.
#
#
# 2. The columns 'GTFS Latitude' and 'GTFS Longitude' required further processing in order to be readily consumable for joins (geographic) across different datasets (for example with the traffic and taxi/cab datasets) : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). 'GTFS Latitude' and 'GTFS Longitude' were merged into a single 'Point' geometry (shapely.geometry.Point) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy (though sometimes computationally expensive) joins across datasets using the geometry attributes like, points, lines and polygons.
#
#
# The processed data is saved in geojson format, to enable quick reading in the clean_and_wrangle notebook

# In[4]:


file = root+'transit/Stations.csv'
df_stations = pd.read_csv(file,usecols=['Station ID','GTFS Stop ID','Stop Name','Borough','GTFS Latitude','GTFS Longitude'])
df_stations.info()
df_stations.head()


# In[5]:


df_stations.columns = ['STATION_ID','STOP_ID','STOP_NAME','BOROUGH','LATITUDE','LONGITUDE']
df_stations.info()
df_stations.head()


# #convert to geodataframe

# In[6]:


geometry = [Point(xy) for xy in zip(df_stations.LATITUDE,df_stations.LONGITUDE)]
df_stations = df_stations.drop(['LATITUDE','LONGITUDE'],axis=1)
crs={'init':'epsg:4326'}
geodf_stations = GeoDataFrame(df_stations,crs=crs,geometry=geometry)


# In[7]:


geodf_stations.info()
geodf_stations.head()


# In[8]:


#add a new geometry to geodf_stations of a circle of X miles around each station
#new design uses polygons that will be loaded from a shape file so drawing buffer circles around the stations will not be required
#X = 0.01
#geodf_stations['CIRCLE'] = geodf_stations.geometry.buffer(X)
#geodf_stations.geometry.name
#geodf_stations = geodf_stations.rename(columns={'geometry':'POINT'}).set_geometry('CIRCLE')
#geodf_stations.geometry.name
#geodf_stations.info()
#geodf_stations.head()


# # LOAD STATIONS FROM TRANSIT DATA

# In[9]:


col_func = lambda x:x.strip().upper() in ['STATION']


# In[10]:


#consider exits and entries both or just one of them? and why?
file = root + 'transit/all_turnstile_1617.txt'
transit_df = pd.read_csv(file,header=0,encoding='ISO-8859-1',
                         usecols = col_func,skipinitialspace=True, low_memory=False, squeeze=True)


# In[11]:


transit_df.head()


# In[12]:


transit_df = transit_df.rename(columns=lambda x: x.strip())


# #read multiple files in a loop into dataframes and then concat them

# In[13]:


transit_df = transit_df.drop_duplicates()
transit_df = transit_df.dropna()


# In[14]:


transit_df.head()


# # FUZZY EXTRACT STATION NAMES FOR TRANSIT DATA

# In[15]:


stations_fuzz_1 = [process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.ratio) for station in transit_df]
stations_fuzz_2 = [process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.partial_ratio) for station in transit_df]
stations_fuzz_3 = [process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.token_sort_ratio) for station in transit_df]


# In[16]:


stations_fuzzy = []
for station in transit_df:
    station_fuzz_1 = process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.ratio)
    station_fuzz_2 = process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.partial_ratio)
    station_fuzz_3 = process.extractOne(station,geodf_stations.STOP_NAME, scorer=fuzz.token_sort_ratio)
    stations = {station_fuzz_1[0]:station_fuzz_1[1],station_fuzz_2[0]:station_fuzz_2[1],station_fuzz_3[0]:station_fuzz_3[1]}
    station_max = max(stations.keys(),key=lambda key: stations[key])
    if stations[station_max] > 88:
        stations_fuzzy.append(station_max)
    else:
        stations_fuzzy.append(np.nan)


# In[17]:


st_df = pd.concat([transit_df.reset_index(drop=True),pd.DataFrame(stations_fuzzy,columns=['fuzzy_stop'])],axis=1,ignore_index=True)
st_df.head(15)


# # ADD A COLUMN STATION, IN TURNSTILE FORMAT, TO THE STATIONS GEO DF

# In[18]:


geodf_stations_merged = pd.merge(geodf_stations,st_df.dropna(),how='left',left_on='STOP_NAME',right_on=1)
geodf_stations_merged = geodf_stations_merged.drop(columns=[1]).rename(columns={0:'STATION'})
geodf_stations_merged.info()
geodf_stations_merged.head()


# In[19]:


filename = root+'transit/Stations_geomerged.geojson'
try:
    os.remove(filename)
except OSError:
    pass
geodf_stations_merged.to_file(filename, driver='GeoJSON')

# # LOAD TRAFFIC LINK DATA

# Source : NYC Open data (Traffic Links Data)
#
# Description : Traffic Links (sets of geographic coordinates) each representing a stretch or road/street over which the average speed of traffic is recorded. Useful fields are 'LINK_ID','LINK_POINTS','BOROUGH'
#
# Processing :
#
# Following data issues have been addressed :
#
# 1. The column 'LINK_POINTS' required further processing in order to be readily consumable for joins (geographic) with the Stations dataset : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). The contents of 'LINK_POINTS' were merged into a single 'LineString' geometry (shapely.geometry.LineString) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy joins across datasets using the geometry attributes like points, lines and polygons.
#
#
# 2. Association of each 'LINK_ID' with a Station : A circle of customizable radius, centered at each station, representing the 'circles of influence' or zones for each station (represented as a Polygon geometry in the Stations dataset) was used to find intersection with traffic link data. (each link was associated with a station by finding which station-zone the link-line intersects with)
#
#
# The processed data is saved in geojson format, to enable quick reading in the clean_and_wrangle notebook

# In[3]:


file = root + 'traffic/DOT_Traffic_Links.csv'
df_traffic_links = pd.read_csv(file, header=0, index_col='LINK_ID')
df_traffic_links.info()
df_traffic_links.head()

# In[4]:


float_pattern = re.compile('^-?\d*\.\d{4,}$')


def build_single_coord_pair(xy):
    # coord pair xy should be a comma separated string of float values
    xy_arr = xy.split(',')
    if (len(xy_arr) == 2):
        x = xy_arr[0]
        y = xy_arr[1]
        try:
            return (make_float(x), make_float(y))
        except:
            return (np.nan, np.nan)
    else:
        return (np.nan, np.nan)


def make_float(x):
    match = float_pattern.match(x)
    if (match):
        try:
            return float(x)
        except:
            return np.nan
    else:
        return np.nan


def build_coord_tuples(coords_str):
    coords_arr = coords_str.split(' ')
    coord_xy = [build_single_coord_pair(x_comma_y) for x_comma_y in coords_arr]
    return [xy for xy in coord_xy if (~(np.isnan(xy[0]) or np.isnan(xy[1])))]


# #convert to geodataframe

# In[5]:


geometry = [LineString(build_coord_tuples(x)) for x in df_traffic_links.LINK_POINTS]
crs = {'init': 'epsg:4326'}
geodf_traffic_links = GeoDataFrame(df_traffic_links.drop('LINK_POINTS', axis=1), crs=crs, geometry=geometry)
geodf_traffic_links.info()
geodf_traffic_links.head()

# In[6]:


geodf_traffic_links.plot(color='r')
plt.show()

# # JOIN TRANSIT STATIONS WITH TRAFFIC LINKS

# In[7]:


# LOAD STATION GEO DF (ALREADY PROCESSED IN STATIONS NOTEBOOK)
# file = root + 'transit/Stations_geomerged.geojson'
# geodf_stations = GeoDataFrame.from_file(file)[['STATION','geometry']]
# geodf_stations.head()


# In[8]:


# geodf_trststns_trfclinks = sjoin(geodf_stations,geodf_traffic_links,how='inner',op='intersects')


# In[9]:


# geodf_trststns_trfclinks = geodf_trststns_trfclinks.rename(columns={'index_right':'LINK_ID'})
# geodf_trststns_trfclinks.info()
# geodf_trststns_trfclinks.head()


# In[10]:


# geodf_stations.plot()
# geodf_traffic_links.plot(color='r')
# geodf_trststns_trfclinks.plot(color='m')
# plt.show()


# In[11]:


# geodf_trststns_trfclinks.to_file(root+'traffic/Traffic_Links_geomerged.geojson',driver='GeoJSON')


# In[12]:


geodf_traffic_links.to_file(root + 'traffic/Traffic_Links.geojson', driver='GeoJSON')



# # prepare cabs data

# In[3]:


prfrm_basic_prcsng = True
prfrm_geo_prcsng = False


# In[4]:


from dask.distributed import Client
client = Client()


# # LOAD AND CLEAN CAB DATA

# Source : NYC TLC (Taxi and Cab Trip Data : every taxi/cab trip in NYC for 2016 and 2017)
#
# Description : Every taxi/cab trip in NYC. Useful fields are 'dropoff_datetime', 'dropoff_latitude', 'dropoff_longitude' , 'pickup_datetime', 'pickup_latitude', 'pickup_longitude', 'passenger_count'
#
# Processing :
#
# Following data issues have been addressed :
#
# 1. The dataset for 2016 and 2017 is too large and called for parallel processing techniques : The python API Dask was leveraged for this. This partitions large datsets into multiple pandas DataFrames and allows for parallel processing on them.
#
#
# 2. The columns 'dropoff_latitude', 'dropoff_longitude' (and 'pickup_latitude', 'pickup_longitude') required further processing in order to be readily consumable for joins (geographic) with the Stations dataset : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). 'dropoff_latitude', 'dropoff_longitude' were merged into a single 'Point' geometry (shapely.geometry.Point) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy (though computationally expensive in this case due to the size of the dataset) joins across datasets using the geometry attributes like, points, lines and polygons. A circle of customizable radius, centered at each station, representing the 'circles of influence' or zones for each station will be used to find intersection with taxi/cab data. (each trip will be associated with a station for the pickup point, as well as a station for the dropoff point, by finding which station-zone the points fall in)
#
#
# The processed data is saved in parquet format, to enable quick reading by dask in the clean_and_wrangle notebook

# In[5]:


if prfrm_basic_prcsng:
    file = root + 'cabs/all_green_1617.csv'


# In[6]:


if prfrm_basic_prcsng:
    df_green = dd.read_csv(file,header=0,
                       usecols = ['Lpep_dropoff_datetime','Passenger_count','Dropoff_longitude','Dropoff_latitude'],
                       skipinitialspace=True,
                       dtype={'Dropoff_latitude': 'object',
                           'Dropoff_longitude': 'object',
                           'Passenger_count': 'object'}
    )[['Lpep_dropoff_datetime','Passenger_count','Dropoff_longitude','Dropoff_latitude']]


# In[7]:


if prfrm_basic_prcsng:
    df_green.info()
    df_green.head()


# In[8]:


if prfrm_basic_prcsng:
    df_green = df_green.rename(columns={'Lpep_dropoff_datetime':'dropoff_datetime','Passenger_count':'passenger_count','Dropoff_longitude':'longitude','Dropoff_latitude':'latitude'})
    df_green.info()
    df_green.head()


# In[9]:


if prfrm_basic_prcsng:
    file = root + 'cabs/all_yellow_1617.csv'


# In[10]:


if prfrm_basic_prcsng:
    df_yellow = dd.read_csv(file,header=0,
                       usecols = ['tpep_dropoff_datetime','passenger_count','dropoff_longitude','dropoff_latitude'],
                       skipinitialspace=True,
                       dtype={'dropoff_latitude': 'object',
                           'dropoff_longitude': 'object',
                           'passenger_count': 'object'})[['tpep_dropoff_datetime','passenger_count','dropoff_longitude','dropoff_latitude']]
    df_yellow.info()
    df_yellow.head()


# In[11]:


if prfrm_basic_prcsng:
    df_yellow = df_yellow.rename(columns={'tpep_dropoff_datetime':'dropoff_datetime','passenger_count':'passenger_count','dropoff_longitude':'longitude','dropoff_latitude':'latitude'})
    df_yellow.info()
    df_yellow.head()


# In[12]:


#cabs_df = pd.concat([df_green,df_yellow],ignore_index=True)
if prfrm_basic_prcsng:
    df_green = df_green.repartition(npartitions=100)
    df_yellow = df_yellow.repartition(npartitions=100)
    cabs_df = dd.concat([df_green,df_yellow])


# In[13]:


#cabs_df.astype(dtype={'dropoff_datetime':'datetime64','passenger_count':'int64','longitude':'float64','latitude':'float64'})
if prfrm_basic_prcsng:
    cabs_df['dropoff_datetime'] = cabs_df['dropoff_datetime'].map_partitions(lambda x: pd.to_datetime(x,format="%Y-%m-%d %H:%M:%S", errors='coerce'),meta=('dropoff_datetime','datetime64[ns]'))


# In[14]:


if prfrm_basic_prcsng:
    cabs_df['passenger_count'] = cabs_df['passenger_count'].map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('passenger_count','int64'))
    cabs_df['longitude'] = cabs_df['longitude'].map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('longitude','float64'))
    cabs_df['latitude'] = cabs_df['latitude'].map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('latitude','float64'))
    cabs_df.info()


# In[15]:


if prfrm_basic_prcsng:
    cabs_df['passenger_count'] = cabs_df['passenger_count'].fillna(1)
    cabs_df['longitude'] = cabs_df['longitude'].fillna(0)
    cabs_df['latitude'] = cabs_df['latitude'].fillna(0)


# In[16]:


if prfrm_basic_prcsng:
    cabs_df = cabs_df.dropna()


# In[17]:


if prfrm_basic_prcsng:
    cabs_df = cabs_df.set_index('dropoff_datetime')


# In[18]:


if prfrm_basic_prcsng:
    cabs_df.to_parquet(root+'cabs',
    has_nulls=False,
    object_encoding='json', compression='SNAPPY')



# # LOAD TRANSIT STATION DATA

# Source : NYC MTA (Subway Stations Data)
# 
# Description : Description of all the subway stations in NYC. Useful fields are 'STOP_NAME' (station name) and 'GTFS Latitude' and 'GTFS Longitude' (geographic coordinates of the station)
# 
# Processing : This dataset has been processed by the stations.ipynb python notebook and saved to Stations_geomerged.geojson
# 
# Following data issues have been addressed :
# 
# 1. There is no unique identifier that represents stations across the NYC MTA database : The 'STATION' column of the MTA turnstile dataset is the only identifier for the station in that set. The contents of this column differed significantly from the 'STOP_NAME' column of the stations dataset. For example, the station named 'TIMES SQ-42 ST' in one set was represented as 'Times Sq - 42 St' in the other. Although, issues like this were easy to fix, there were a fair number of cases where a station named 'Astoria - Ditmars Blvd' did not have any obvious match in the other data set. A possible cause for cases like this is the use of different station names for the same station ('Astoria - Ditmars Blvd' station was earlier known as 'Second Avenue'). Cases like this are hard, if not impossible to match. A python string-matching library called 'fuzzy-wuzzy' was used to find the best matches using 3 Levenshtein closeness ratios (normal ratio, partial ratio and token sort ratio). The match was accepted only if one of the three matching methods returned a ratio of 88% or higher. The matching station names from the trunstile dataset were added to the 'STATION' column of the stations dataset.
# 
#     
# 2. The columns 'GTFS Latitude' and 'GTFS Longitude' required further processing in order to be readily consumable for joins (geographic) across different datasets (for example with the traffic and taxi/cab datasets) : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). 'GTFS Latitude' and 'GTFS Longitude' were merged into a single 'Point' geometry (shapely.geometry.Point) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy (though sometimes computationally expensive) joins across datasets using the geometry attributes like, points, lines and polygons. A circle of customizable radius, centered at each station, was also drawn and added to a new geometry column containing the circles as polygons. These circles represent 'circles of influence' or zones for each station and will be used to find intersection with traffic and taxi/cab data.

# In[3]:


file = root + 'transit/Stations_geomerged.geojson'
geodf_stations = GeoDataFrame.from_file(file)
geodf_stations.head()


# # LOAD AND CLEAN TRANSIT DATA

# Source : NYC MTA (Subway Stations Turnstile Data : 4-hour frequency, 2016 and 2017)
# 
# Description : Transit ridership (turnstile entry and exit counts) of all the subway stations in NYC. Useful fields are 'STATION' (station name), 'DATE' (date) , 'TIME' (time) , 'ENTRIES' (entry count) , 'EXITS' (exit count)
# 
# Processing : 
# 
# Following data issues have been addressed :
# 
# 1. 'DATE' and 'TIME' occur as separate string columns : These two were merged and converted to type 'datetime64[ns]'. This column was also used as the index (after the rest of cleaning was complete)
# 
# 
# 2. 'EXITS' (and 'ENTRIES') columns have cumulative reading of the turnstile unit : the pandas.Series.diff method was used to calculate the change from the previous reading. 
# 
# 
# 3. Turnstile units would reset randomly once in a while, resulting in outliers in the 'EXITS' (and 'ENTRIES') columns (abmormally high values or negative values) : These outliers were identified and filtered out by calculating the inter-quartile range and rejecting all rows with 'EXITS' (or 'ENTRIES') with values greater that 5 times the inter-quartile range or with negative values. 

# In[4]:


#taking 2 arguments date and time instead of single datetime improved performance drastically
def parse_date(date,time):
    return pd.to_datetime(str(date)+str(time),format="%m/%d/%Y%H:%M:%S", errors='coerce')
def parse_int(exits):
    return pd.to_numeric(exits,errors='coerce')


# In[5]:


col_func = lambda x:x.strip().upper() in ['STATION','DATE','TIME','EXITS']


# In[6]:


#consider exits and entries both or just one of them? and why?
file = root + 'transit/all_turnstile_1617.txt'
transit_df = pd.read_csv(file,encoding= 'ISO-8859-1', header=0,parse_dates={'DATETIME': ['DATE','TIME']},
                         usecols = col_func,skipinitialspace=True, low_memory=False,
                         date_parser=parse_date)


# In[7]:


transit_df.info()
transit_df.head()


# In[8]:


transit_df = transit_df.rename(columns=lambda x: x.strip())


# #read multiple files in a loop into dataframes and then concat them

# In[9]:


transit_df = transit_df.drop_duplicates()
transit_df = transit_df.dropna()


# In[10]:


transit_df['EXITS'] = pd.to_numeric(transit_df['EXITS'],errors='coerce')
transit_df = transit_df.set_index('DATETIME')
transit_df.info()


# In[11]:


transit_df['DELEXITS']= transit_df['EXITS'].diff()
#transit_df['EXITS'] = transit_df.apply(fix_exits,axis=1,iqr=intqrange)


# In[12]:


transit_df.info()


# In[13]:


intqrange = transit_df['DELEXITS'].quantile(0.75) - transit_df['DELEXITS'].quantile(0.25)
discard = (transit_df['DELEXITS'] < 0) | (transit_df['DELEXITS'] > 5*intqrange)
transit_df = transit_df.loc[~discard]
transit_df = transit_df.dropna()


# In[14]:


transit_df.info()


# In[15]:


transit_df.describe()


# # LOAD CAB DATA

# Source : NYC TLC (Taxi and Cab Trip Data : every taxi/cab trip in NYC for 2016 and 2017)
# 
# Description : Every taxi/cab trip in NYC. Useful fields are 'dropoff_datetime', 'dropoff_latitude', 'dropoff_longitude' , 'pickup_datetime', 'pickup_latitude', 'pickup_longitude', 'passenger_count'
# 
# Processing :
# 
# Following data issues have been addressed :
# 
# 1. The dataset for 2016 and 2017 is too large and called for parallel processing techniques : The python API Dask was leveraged for this. This partitions large datsets into multiple pandas DataFrames and allows for parallel processing on them.
# 
# 
# 2. The columns 'dropoff_latitude', 'dropoff_longitude' (and 'pickup_latitude', 'pickup_longitude') required further processing in order to be readily consumable for joins (geographic) with the Stations dataset : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). 'dropoff_latitude', 'dropoff_longitude' were merged into a single 'Point' geometry (shapely.geometry.Point) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy (though computationally expensive in this case due to the size of the dataset) joins across datasets using the geometry attributes like, points, lines and polygons. A circle of customizable radius, centered at each station, representing the 'circles of influence' or zones for each station will be used to find intersection with taxi/cab data. (each trip will be associated with a station for the pickup point, as well as a station for the dropoff point, by finding which station-zone the points fall in)
# 
#     
# The processed data is saved in parquet format (processed and saved by the cabs notebook), to enable quick reading by dask in the clean_and_wrangle notebook

# In[16]:


cabs_df = dd.read_parquet(root+'cabs')
cabs_df.info()
#cabs_df.head()


# # LOAD TRAFFIC LINK DATA

# Source : NYC Open data (Traffic Links Data)
# 
# Description : Traffic Links (sets of geographic coordinates) each representing a stretch or road/street over which the average speed of traffic is recorded. Useful fields are 'LINK_ID','LINK_POINTS','BOROUGH'
# 
# Processing :
# 
# Following data issues have been addressed :
# 
# 1. The column 'LINK_POINTS' required further processing in order to be readily consumable for joins (geographic) with the Stations dataset : The python geopandas library was leveraged for this purpose (This library in turn depends on shapely, fiona and rtree). The contents of 'LINK_POINTS' were merged into a single 'LineString' geometry (shapely.geometry.LineString) and the entire datset converted to a geopandas GeoDataFrame. This allows for fairly easy joins across datasets using the geometry attributes like points, lines and polygons. 
# 
# 
# 2. Association of each 'LINK_ID' with a Station : A circle of customizable radius, centered at each station, representing the 'circles of influence' or zones for each station (represented as a Polygon geometry in the Stations dataset) was used to find intersection with traffic link data. (each link was associated with a station by finding which station-zone the link-line intersects with)
# 
#     
# The processed data is saved in geojson format, to enable quick reading in the clean_and_wrangle notebook

# In[17]:


file = root + 'traffic/Traffic_Links.geojson'
geodf_traffic_links = GeoDataFrame.from_file(file)
geodf_traffic_links.head()


# # LOAD AND CLEAN TRAFFIC DATA

# Source : NYC Open data (Traffic speed data Data recorded at various locations in NYC for 2016 and 2017)
# 
# Description : Traffic speed data Data recorded at various locations in NYC. Useful fields are 'LINK_ID','DATETIME','SPEED'
# 
# Processing :
# 
# No issues found with this set so far.

# In[18]:


file = root + 'traffic/DOT_Traffic_1617_hourly.csv'
df_traffic = pd.read_csv(file, header=None,parse_dates=[1])
df_traffic.columns = ['LINK_ID','DATETIME','SPEED']
#index by datetime
df_traffic = df_traffic.set_index('DATETIME')
df_traffic.info()
df_traffic.head()


# # LOAD AND CLEAN WEATHER DATA

# Source : National Climatic Data Center (daily temperature, rainfall and snowfall data for NYC for 2016 and 2017)
# 
# Description : Daily temperature, rainfall and snowfall data for NYC. Useful fields are 'DATE','PRCP','SNOW','TMAX','TMIN'
# 
# Processing :
# 
# Following data issues have been addressed :
# 
# 1. 'DATE' in string format : This was converted to type 'datetime64[ns]'. This column was also used as the index (after the rest of cleaning was complete)
# 
# 
# 2. Average daily temperature : This was calculated by finding the mean of 'TMIN' and 'TMAX' and added as 'TAVG'

# In[19]:


def parse_date_3(date):
    return pd.to_datetime(date,format="%Y-%m-%d", errors='coerce')


# In[20]:


#weather data
file = root + 'weather/weather.csv'
weather_df = pd.read_csv(file,header=0,parse_dates=['DATE'],
                         usecols=['DATE','PRCP','SNOW','TMAX','TMIN'],
                         skipinitialspace=True,
                         date_parser=parse_date_3)
weather_df.info()


# In[21]:


weather_df = weather_df.dropna().set_index('DATE')
weather_df = weather_df.loc['2016-01-01':'2017-12-31']
weather_df.info()


# In[22]:


weather_df['TAVG'] = (weather_df['TMIN'] + weather_df['TMAX']) / 2


# # LOAD AND CLEAN GAS PRICE DATA

# Source : https://www.nyserda.ny.gov (monthly gas prices for NYC for 2016 and 2017)
# 
# Description : Monthly gas prices for NYC.
# 
# Processing :
# 
# Following data issues have been addressed :
# 
# 1. Each year is a different column : The dataframe was melted to create a single column called 'YEAR'. The 'MONTH' and 'YEAR' were then combined and converted to datetime and used as index.

# In[23]:


file = root + 'gas/gas.csv'
gas_df = pd.read_csv(file, header=0, skipinitialspace=True)
gas_df.info()
gas_df


# In[24]:


gas_df = gas_df.rename(columns={'Unnamed: 0':'MONTH'})
gas_df.columns


# In[25]:


gas_df = gas_df.melt(id_vars=['MONTH'],var_name='YEAR',value_name='PRICE')
gas_df.head()


# In[26]:


gas_df['MONTH'] = pd.to_datetime(gas_df['MONTH']+'-'+gas_df['YEAR'],format='%b-%Y')
del gas_df['YEAR']

gas_df.head()


# In[27]:


gas_df = gas_df.set_index('MONTH')
gas_df.head()


# In[28]:


gas_df = gas_df.sort_index().loc['2016-01-01':'2017-12-31']
gas_df.head()


# # FILTER ALL TIME SERIES DATASETS BY DAY OF WEEK AND RESAMPLE

# Wrangling of each of the time series datasets created/loaded above :
# 
# 1. Filter by day of the week : The time-series datasets for transit, traffic, cabs and weather were filtered by day of the week (this is because of the fact that datasets like transit and traffic, for instance, would have different patterns on weekdays than on weekends and it therefore makes sense to seperate out and compare trends by day of the week)
# 
#     
# 2. Aggregate the filtered data over 1 month window : All time-series datasets (except gas which is already aggregated by month), were resampled using frequency = '1M' and aggregated using mean, median or sum functions

# In[29]:


#week day
weekday = 2

transit_df_byday = transit_df.loc[transit_df.index.weekday==weekday]
transit_df_byday.info()
transit_df_byday.head()


# In[30]:


#transit_df_rsmpld = transit_df.reset_index().set_index('DATETIME').resample('1D',how='sum')
transit_df_rsmpld = transit_df_byday.reset_index().groupby('STATION').apply(lambda x: x.set_index('DATETIME').resample('1M').sum()).swaplevel(1,0)
transit_df_rsmpld.info()
transit_df_rsmpld.head()


# In[31]:


idx = pd.IndexSlice
transit_df_rsmpld = transit_df_rsmpld.sort_index().loc[idx['2016-01-01':'2017-12-31',:],:].sort_index()
transit_df_rsmpld.info()
transit_df_rsmpld.head()


# In[32]:


#weekday = 2
traffic_df = df_traffic.dropna()
traffic_df_byday = traffic_df.loc[traffic_df.index.weekday==weekday]
traffic_df_byday.info()
traffic_df_byday.head()


# In[33]:


traffic_df_rsmpld = traffic_df_byday.reset_index().groupby('LINK_ID').apply(lambda x: x.set_index('DATETIME').resample('1M').median()).swaplevel(1,0)
traffic_df_rsmpld.info()
traffic_df_rsmpld.head()


# In[34]:


#idx = pd.IndexSlice
traffic_df_rsmpld = traffic_df_rsmpld.sort_index().loc[idx['2016-01-01':'2017-12-31',:],:].sort_index()
traffic_df_rsmpld.info()
traffic_df_rsmpld.head()


# In[57]:


cabs_df[:2]
#cabs_df.info()


# In[61]:


cabs_df = cabs_df.repartition(npartitions=600)


# In[63]:


#resample transit and cab data to a lower frequency (1 Month)

#select by day
cabs_df = cabs_df.loc['2016-01-01':'2017-12-31']
#cabs_df.compute()
#cabs_df_byday = cabs_df.loc[cabs_df.index.dt.dayofweek == weekday]
cabs_df[:2]
#cabs_df_byday.head()


# In[66]:


#cabs_df.groupby(['latitude','longitude']).resample('1M').count()
cabs_df = cabs_df.reset_index().groupby(['latitude','longitude']).apply(lambda x: x.set_index('dropoff_datetime').resample('1M').count())
cabs_df[:2]


# In[ ]:


cabs_df.compute()


# In[60]:


#cabs_df_rsmpld = cabs_df.resample('1M').count()
#transit_df = transit_df.reset_index().set_index('DATETIME')
#transit_df_rsmpld = transit_df_rsmpld.reset_index().set_index(['DATETIME','STATION'])
#cabs_df_rsmpld.info()
cabs_df.head()


# In[ ]:


#select by day
weather_df_byday = weather_df.loc[weather_df.index.weekday == weekday]
weather_df_byday.info()
weather_df_byday.head()


# In[ ]:


weather_df_rsmpld = weather_df_byday.resample('1M').mean()
#weather_snow = weather_df['SNOW'].resample('1M').mean()
#weather_snow = weather_df['SNOW'].resample('1M').mean()

weather_df_rsmpld.head()


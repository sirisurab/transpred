import pandas as pd
import dask.dataframe as dd
root = 'data/'

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

# read data into dask dataframes
file = root + '/data/gcabs/*.csv'

df_green = dd.read_csv(file,header=0,
                   usecols = ['Lpep_dropoff_datetime',
                              'Passenger_count',
                              'Dropoff_longitude',
                              'Dropoff_latitude'],
                   skipinitialspace=True,
                   dtype={'Dropoff_latitude': 'object',
                       'Dropoff_longitude': 'object',
                       'Passenger_count': 'object'}
)[['Lpep_dropoff_datetime',
   'Passenger_count',
   'Dropoff_longitude',
   'Dropoff_latitude']]

df_green = df_green.rename(columns={'Lpep_dropoff_datetime':'dropoff_datetime',
                                    'Passenger_count':'passenger_count',
                                    'Dropoff_longitude':'longitude',
                                    'Dropoff_latitude':'latitude'})


file = root + '/data/ycabs/*.csv'

df_yellow = dd.read_csv(file,header=0,
                   usecols = ['tpep_dropoff_datetime',
                              'passenger_count',
                              'dropoff_longitude',
                              'dropoff_latitude'],
                   skipinitialspace=True,
                   dtype={'dropoff_latitude': 'object',
                       'dropoff_longitude': 'object',
                       'passenger_count': 'object'}
                        )[['tpep_dropoff_datetime',
                           'passenger_count',
                           'dropoff_longitude',
                           'dropoff_latitude']]



df_yellow = df_yellow.rename(columns={'tpep_dropoff_datetime':'dropoff_datetime',
                                      'passenger_count':'passenger_count',
                                      'dropoff_longitude':'longitude',
                                      'dropoff_latitude':'latitude'})


#df_green = df_green.repartition(npartitions=100)
#df_yellow = df_yellow.repartition(npartitions=100)
# TODO
cabs_df = dd.concat([df_green,df_yellow])

# per-line cleaning

cabs_df['dropoff_datetime'] = cabs_df['dropoff_datetime']\
    .map_partitions(lambda x: pd.to_datetime(x,format="%Y-%m-%d %H:%M:%S", errors='coerce'),
                    meta=('dropoff_datetime','datetime64[ns]'))


cabs_df['passenger_count'] = cabs_df['passenger_count']\
    .map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('passenger_count','int64'))
cabs_df['longitude'] = cabs_df['longitude']\
    .map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('longitude','float64'))
cabs_df['latitude'] = cabs_df['latitude']\
    .map_partitions(lambda x: pd.to_numeric(x, errors='coerce'),meta=('latitude','float64'))

cabs_df['passenger_count'] = cabs_df['passenger_count'].fillna(1)
cabs_df['longitude'] = cabs_df['longitude'].fillna(0)
cabs_df['latitude'] = cabs_df['latitude'].fillna(0)


cabs_df = cabs_df.dropna()


#TODO
cabs_df = cabs_df.set_index('dropoff_datetime')

cabs_df.compute()

cabs_df.to_parquet(root+'cabs',
has_nulls=False,
object_encoding='json', compression='SNAPPY')
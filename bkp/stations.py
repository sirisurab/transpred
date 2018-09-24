import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
root = 'data/'

from geopandas import GeoDataFrame
from shapely.geometry import Point


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


df_stations.columns = ['STATION_ID','STOP_ID','STOP_NAME','BOROUGH','LATITUDE','LONGITUDE']

# convert to geodataframe

geometry = [Point(xy) for xy in zip(df_stations.LATITUDE,df_stations.LONGITUDE)]
df_stations = df_stations.drop(['LATITUDE','LONGITUDE'],axis=1)
crs={'init':'epsg:4326'}
geodf_stations = GeoDataFrame(df_stations,crs=crs,geometry=geometry)

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


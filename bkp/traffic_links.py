
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
root = 'data/'


# In[2]:


from geopandas import GeoDataFrame
from shapely.geometry import LineString


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
df_traffic_links = pd.read_csv(file, header=0,index_col='LINK_ID')
df_traffic_links.info()
df_traffic_links.head()


# In[4]:


float_pattern = re.compile('^-?\d*\.\d{4,}$')
def build_single_coord_pair(xy):
    #coord pair xy should be a comma separated string of float values
    xy_arr = xy.split(',')
    if (len(xy_arr) == 2):
        x = xy_arr[0]
        y = xy_arr[1]
        try:
            return (make_float(x),make_float(y))
        except:
            return (np.nan,np.nan)
    else:
        return (np.nan,np.nan)
    
def make_float(x):
    match = float_pattern.match(x)
    if(match):
        try:
            return float(x)
        except:
            return np.nan
    else:
        return np.nan
                
def build_coord_tuples(coords_str):
    coords_arr = coords_str.split(' ')
    coord_xy = [build_single_coord_pair(x_comma_y) for x_comma_y in coords_arr]
    return [xy for xy in coord_xy if (~( np.isnan(xy[0]) or np.isnan(xy[1]) ) ) ]


# #convert to geodataframe

# In[5]:


geometry = [LineString(build_coord_tuples(x)) for x in df_traffic_links.LINK_POINTS]
crs={'init':'epsg:4326'}
geodf_traffic_links = GeoDataFrame(df_traffic_links.drop('LINK_POINTS',axis=1),crs=crs,geometry=geometry)
geodf_traffic_links.info()
geodf_traffic_links.head()


# In[6]:


geodf_traffic_links.plot(color='r')
plt.show()


# # JOIN TRANSIT STATIONS WITH TRAFFIC LINKS

# In[7]:


#LOAD STATION GEO DF (ALREADY PROCESSED IN STATIONS NOTEBOOK)
#file = root + 'transit/Stations_geomerged.geojson'
#geodf_stations = GeoDataFrame.from_file(file)[['STATION','geometry']]
#geodf_stations.head()


# In[8]:


#geodf_trststns_trfclinks = sjoin(geodf_stations,geodf_traffic_links,how='inner',op='intersects')


# In[9]:


#geodf_trststns_trfclinks = geodf_trststns_trfclinks.rename(columns={'index_right':'LINK_ID'})
#geodf_trststns_trfclinks.info()
#geodf_trststns_trfclinks.head()


# In[10]:


#geodf_stations.plot()
#geodf_traffic_links.plot(color='r')
#geodf_trststns_trfclinks.plot(color='m')
#plt.show()


# In[11]:


#geodf_trststns_trfclinks.to_file(root+'traffic/Traffic_Links_geomerged.geojson',driver='GeoJSON')


# In[12]:


geodf_traffic_links.to_file(root+'traffic/Traffic_Links.geojson',driver='GeoJSON')


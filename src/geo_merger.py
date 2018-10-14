import sys
from data_tools import file_io
from geopandas import GeoDataFrame, sjoin
from pandas import DataFrame
from math import sin, pi
from bokeh.plotting import figure, output_file, show
from bokeh.models import GeoJSONDataSource
import matplotlib.pyplot as plt
from utils import persistence as ps

GEOMERGED_PATH: str = 'geo-merged/'
REFBASE_BUCKET: str = 'ref-base'
NYC_LATITUDE: float = 40.7128
PLOTS_BUCKET: str = 'plots'


def geo_merge(buffer_radius: float) -> bool:

    try:
        # load station data
        st_zipname: str = 'stations.zip'
        st_filename: str = 'stations.shp'
        stations_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=st_filename,
                                                                  zipname=st_zipname,
                                                                  bucket=REFBASE_BUCKET)

        # covert buffer_radius from miles to degrees
        gc_radius_miles: float = 3963 - 13 * sin(NYC_LATITUDE * pi/180)
        buffer_degrees: float = buffer_radius / gc_radius_miles

        # add circular buffer around each station
        stations_df['circle'] = stations_df.geometry.buffer(buffer_degrees)
        stations_df.rename(columns={'geometry': 'point'}, inplace=True)
        stations_df.set_geometry('circle', inplace=True)

        # load taxi_zones data
        tz_zipname: str = 'taxi_zones.zip'
        tz_filename: str = 'taxi_zones.shp'
        taxi_zone_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=tz_filename,
                                                                  zipname=tz_zipname,
                                                                  bucket=REFBASE_BUCKET)
        # perform spatial join
        # between stations (buffer circles) and taxi-zones polygons
        stations_cabs_df: GeoDataFrame = sjoin(stations_df, taxi_zone_df, how='left', op='intersects')

        # write joined file (as csv without geometry columns) to geo-merged bucket
        stations_cabs_df = stations_cabs_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'LocationID']]
        stations_cabs_df.rename(columns={'LocationID': 'locationid'}, inplace=True)
        geomerged_file: str = GEOMERGED_PATH+str(buffer_radius)+'/cabs.csv'
        status_1: bool = file_io.write_csv(df=stations_cabs_df, bucket=REFBASE_BUCKET, filename=geomerged_file)

        # load traffic_links data
        tl_zipname: str = 'traffic_links.zip'
        tl_filename: str = 'traffic_links.shp'
        links_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=tl_filename,
                                                                  zipname=tl_zipname,
                                                                  bucket=REFBASE_BUCKET)
        # perform spatial join
        # between stations (buffer circles) and traffic_links lines
        stations_traffic_df: GeoDataFrame = sjoin(stations_df, links_df, how='left', op='intersects')

        # write joined file (as csv without geometry columns) to geo-merged bucket
        stations_traffic_df = stations_traffic_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'linkid']]
        #df.rename(columns={'LocationID': 'dolocationid'}, inplace=True)
        geomerged_file = GEOMERGED_PATH + str(buffer_radius) + '/traffic.csv'
        status_2: bool = file_io.write_csv(df=stations_traffic_df, bucket=REFBASE_BUCKET, filename=geomerged_file)

        # create plots
        stations_df.plot()
        taxi_zone_df.plot()
        links_df.plot()
        stations_cabs_df.plot()
        stations_traffic_df.plot()
        plt.show()
        plotfilepath: str = '/tmp/'
        plotfilename: str = 'geomerged'+str(buffer_radius)+'.png'
        plt.savefig(plotfilepath+plotfilename)
        status_3: bool = ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plotfilename, source=plotfilepath+plotfilename)


    except Exception as err:
        print('Error in geo_merge %(radius)s' % {'radius': str(buffer_radius)})
        raise err

    else:
        return status_1 and status_2


if __name__ == '__main__':
    print('performing geographic data-merges for buffer radius %s' % sys.argv[1])
    try:
        buffer_radius_miles: float = float(sys.argv[1])
    except TypeError as err:
        print('incorrect type for buffer radius input to geo_merger.py, '
              'expecting a floating point value but received %s' % sys.argv[1])
    else:
        status: bool = geo_merge(buffer_radius=buffer_radius_miles)

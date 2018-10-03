import sys
from data_tools import file_io
from geopandas import GeoDataFrame, sjoin
from pandas import DataFrame
from math import sin, pi

GEOMERGED_BUCKET: str = 'geo-merged'
REFBASE_BUCKET: str = 'ref-base'
NYC_LATITUDE: float = 40.7128


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
        stations_df = sjoin(stations_df, taxi_zone_df, how='left', op='intersects')

        # write joined file (as csv without geometry columns) to geo-merged bucket
        df: DataFrame = stations_df[['station_id', 'stop_id', 'stop_name', 'turnstile_station', 'borough', 'LocationID']]
        df.rename(columns={'LocationID': 'dolocationid'}, inplace=True)
        geomerged_file: str = 'geomerged_'+str(buffer_radius)+'.csv'
        status: bool = file_io.write_csv(df=df, bucket=GEOMERGED_BUCKET, filename=geomerged_file)

    except Exception as err:
        print('Error in geo_merge %(radius)s' % {'radius': str(buffer_radius)})
        raise err

    else:
        return status


if __name__ == '__main__':
    print('performing geographic data-merges for buffer radius %s' % sys.argv[1])
    try:
        buffer_radius_miles: float = float(sys.argv[1])
    except TypeError as err:
        print('incorrect type for buffer radius input to geo_merger.py, '
              'expecting a floating point value but received %s' % sys.argv[1])
    else:
        status: bool = geo_merge(buffer_radius=buffer_radius_miles)

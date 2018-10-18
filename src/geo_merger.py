import sys
from geopandas import GeoDataFrame, sjoin
from math import sin, pi
import matplotlib.pyplot as plt
from utils import persistence as ps, file_io
from typing import List

GEOMERGED_PATH: str = 'geo-merged/'
REFBASE_BUCKET: str = 'ref-base'
NYC_LATITUDE: float = 40.7128
PLOTS_BUCKET: str = 'plots'


def geo_merge(buffer_radii: List[str]) -> bool:
    gc_radius_miles: float = 3963 - 13 * sin(NYC_LATITUDE * pi/180)

    # load station data
    st_zipname: str = 'stations.zip'
    st_filename: str = 'stations.shp'
    stations_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=st_filename,
                                                             zipname=st_zipname,
                                                             bucket=REFBASE_BUCKET)

    # load taxi_zones data
    tz_zipname: str = 'taxi_zones.zip'
    tz_filename: str = 'taxi_zones.shp'
    taxi_zone_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=tz_filename,
                                                              zipname=tz_zipname,
                                                              bucket=REFBASE_BUCKET)

    # load traffic_links data
    tl_zipname: str = 'traffic_links.zip'
    tl_filename: str = 'traffic_links.shp'
    links_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=tl_filename,
                                                          zipname=tl_zipname,
                                                          bucket=REFBASE_BUCKET)

    for radius in buffer_radii:
        try:
            buffer_radius_miles: float = float(radius)
        except TypeError as err:
            print('incorrect type for buffer radius input to geo_merger.py, '
                  'expecting a floating point value but received %s' % radius)
        else:
            try:
                print('performing geographic data-merges for buffer radius %s' % buffer_radius_miles)

                # covert buffer_radius from miles to degrees
                buffer_degrees: float = buffer_radius_miles / gc_radius_miles

                # add circular buffer around each station
                stations_geodf = stations_df.copy()
                stations_geodf['circle'] = stations_geodf.geometry.buffer(buffer_degrees)
                stations_geodf.rename(columns={'geometry': 'point'}, inplace=True)
                stations_geodf.set_geometry('circle', inplace=True)

                # create plots
                fig, ax = plt.subplots()
                ax.set_aspect('equal')
                stations_geodf.plot(ax=ax, color='c', column='circle', alpha=0.6)


                # plot
                taxi_zone_df.plot(ax=ax, color='y', alpha=0.3)

                # perform spatial join
                # between stations (buffer circles) and taxi-zones polygons
                stations_cabs_df: GeoDataFrame = sjoin(stations_geodf, taxi_zone_df, how='left', op='intersects')

                # plot
                #stations_cabs_df.plot(ax=ax, color='y')

                # write joined file (as csv without geometry columns) to geo-merged bucket
                stations_cabs_df = stations_cabs_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'LocationID']]
                stations_cabs_df.rename(columns={'LocationID': 'locationid'}, inplace=True)
                geomerged_file: str = GEOMERGED_PATH+str(buffer_radius_miles)+'/cabs.csv'
                status_1: bool = file_io.write_csv(df=stations_cabs_df, bucket=REFBASE_BUCKET, filename=geomerged_file)


                # perform spatial join
                # between stations (buffer circles) and traffic_links lines
                stations_traffic_df: GeoDataFrame = sjoin(stations_geodf, links_df, how='left', op='intersects')

                # plot
                links_df.plot(ax=ax, color='m')
                #stations_traffic_df.plot(ax=ax, color='c')

                # write joined file (as csv without geometry columns) to geo-merged bucket
                stations_traffic_df = stations_traffic_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'linkid']]
                #df.rename(columns={'LocationID': 'dolocationid'}, inplace=True)
                geomerged_file = GEOMERGED_PATH + str(buffer_radius_miles) + '/traffic.csv'
                status_2: bool = file_io.write_csv(df=stations_traffic_df, bucket=REFBASE_BUCKET, filename=geomerged_file)


                # save plots
                plt.show()
                plotfilepath: str = '/tmp/'
                plotfilename: str = 'geomerged'+str(buffer_radius_miles)+'.png'
                plt.savefig(plotfilepath+plotfilename)
                status_3: bool = ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plotfilename, source=plotfilepath+plotfilename)


            except Exception as err:
                print('Error in geo_merge %(radius)s' % {'radius': str(buffer_radius_miles)})
                raise err


    return True


if __name__ == '__main__':
    print('performing geographic data-merges for buffer radii %s' % sys.argv[1:])
    buffer_radii = sys.argv[1:]
    status: bool = geo_merge(buffer_radii=buffer_radii)

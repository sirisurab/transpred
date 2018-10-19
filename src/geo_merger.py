import sys
from geopandas import GeoDataFrame, sjoin
from math import sin, pi
import matplotlib.pyplot as plt
from utils import persistence as ps, file_io
from geog import propagate
from numpy import linspace, arange, ndarray
import seaborn as sns
from shapely.geometry import Polygon
from typing import Tuple, List
from pandas import Series, DataFrame
from functools import partial

GEOMERGED_PATH: str = 'geo-merged/'
REFBASE_BUCKET: str = 'ref-base'
NYC_LATITUDE: float = 40.7128
PLOTS_BUCKET: str = 'plots'
METERS_PER_MILE: float = 1609.34
GEOG_N_POINTS: int = 20


def make_plots(buffer_radius_miles: float, stations_geodf: GeoDataFrame, taxi_zone_df: GeoDataFrame, links_df: GeoDataFrame) -> bool:
    sns.set()
    sns.set_style('darkgrid')
    # create plots
    fig, ax = plt.subplots(1, figsize=(18, 18), clear=True)
    # taxi zones plot
    taxi_zone_df.plot(ax=ax, facecolor='#F9DA95', edgecolor='#FFFFFF', linewidth=0.5)

    stations_geodf.plot(ax=ax, facecolor='#618A98', edgecolor='#618A98', alpha=0.2/buffer_radius_miles)
    stations_points_geodf = stations_geodf.copy().set_geometry('point').drop(columns=['circle'])
    stations_points_geodf.plot(ax=ax, color='#787064', markersize=.5)
    links_df.plot(ax=ax, color='#AE4B16', linewidth=0.5)

    # save plots
    plt.show()
    plotfilepath: str = '/tmp/'
    plotfilename: str = 'geomerged' + str(buffer_radius_miles) + '.png'
    plt.savefig(plotfilepath + plotfilename)
    status: bool = ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plotfilename, source=plotfilepath + plotfilename)

    return status


def add_weight(row, id_col, prev_buffer_df, buffer_radius):
    if row[id_col] == '' or row[id_col] is None:
        return ''
    if prev_buffer_df.size == 0:
        return buffer_radius
    df_row: DataFrame = prev_buffer_df.loc[(prev_buffer_df[id_col] == row[id_col]) & (prev_buffer_df['station_id'] == row['station_id'])]
    if df_row.size > 0:
        print('row in add_weight for %(id_col)s %(buffer)s' % {'id_col': id_col, 'buffer': str(buffer_radius)})
        print(df_row)
        return df_row['weight'].iloc[0]
    else:
        return buffer_radius


def create_spatial_joins(buffer_radius_miles: float, stations_geodf: GeoDataFrame, taxi_zone_df: GeoDataFrame, links_df: GeoDataFrame, prev_buffer_ids: Tuple[DataFrame, DataFrame]) -> Tuple[DataFrame, DataFrame]:
    # perform spatial join
    # between stations (buffer circles) and taxi-zones polygons
    stations_cabs_df: GeoDataFrame = sjoin(stations_geodf, taxi_zone_df, how='left', op='intersects')
    stations_cabs_df = stations_cabs_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'LocationID']]
    stations_cabs_df.rename(columns={'LocationID': 'locationid'}, inplace=True)
    # perform spatial join
    # between stations (buffer circles) and traffic_links lines
    stations_traffic_df: GeoDataFrame = sjoin(stations_geodf, links_df, how='left', op='intersects')
    stations_traffic_df = stations_traffic_df[['station_id', 'stop_id', 'stop_name', 'tsstation', 'borough', 'linkid']]

    # exclude previous buffer cab location ids and traffic link ids from current buffer circle, before writing to file

    stations_cabs_df['weight'] = stations_cabs_df.apply(func=partial(add_weight,
                                                                     id_col='locationid',
                                                                     prev_buffer_df=prev_buffer_ids[0],
                                                                     buffer_radius=buffer_radius_miles),
                                                        axis=1)
    stations_traffic_df['weight'] = stations_traffic_df.apply(func=partial(add_weight,
                                                                           id_col='linkid',
                                                                           prev_buffer_df=prev_buffer_ids[1],
                                                                           buffer_radius=buffer_radius_miles),
                                                              axis=1)
    print('stations_cabs for buffer %s' % str(buffer_radius_miles))
    print(stations_cabs_df.head(1))
    print('stations_traffic for buffer %s' % str(buffer_radius_miles))
    print(stations_traffic_df.head(1))

    return stations_cabs_df, stations_traffic_df


def geo_merge(buffer_radii: ndarray) -> bool:
    #gc_radius_miles: float = 3963 - 13 * sin(NYC_LATITUDE * pi/180)

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

    status_1: bool = False
    prev_buffer_ids: Tuple[DataFrame, DataFrame] = (DataFrame(), DataFrame())
    for radius in buffer_radii:
        status_1 = False
        try:
            buffer_radius_miles: float = float(radius)
        except TypeError as err:
            print('incorrect type for buffer radius input to geo_merger.py, '
                  'expecting a floating point value but received %s' % radius)
        else:
            try:
                print('performing geographic data-merges for buffer radius %s' % buffer_radius_miles)

                # add circular buffer around each station
                stations_geodf = stations_df.copy()

                # shapely / geopandas buffer
                # convert buffer_radius from miles to degrees
                #buffer_degrees: float = buffer_radius_miles / gc_radius_miles
                #stations_geodf['circle'] = stations_geodf.geometry.buffer(buffer_degrees)

                # geog polygon
                # distance in meters
                distance: float = buffer_radius_miles * METERS_PER_MILE
                angles = linspace(0, 360, GEOG_N_POINTS)
                stations_geodf['circle'] = stations_geodf.geometry.apply(propagate, angle=angles, d=distance)
                stations_geodf['circle'] = stations_geodf['circle'].apply(Polygon)

                stations_geodf = stations_geodf.rename(columns={'geometry': 'point'}).set_geometry('circle')

                # create plots
                status_2: bool = make_plots(buffer_radius_miles=buffer_radius_miles,
                                            stations_geodf=stations_geodf,
                                            taxi_zone_df=taxi_zone_df,
                                            links_df=links_df)

                # perform spatial join
                prev_buffer_ids = create_spatial_joins(buffer_radius_miles=buffer_radius_miles,
                                                       stations_geodf=stations_geodf,
                                                       taxi_zone_df=taxi_zone_df,
                                                       links_df=links_df,
                                                       prev_buffer_ids=prev_buffer_ids)


            except Exception as err:
                print('Error in geo_merge %(radius)s' % {'radius': str(buffer_radius_miles)})
                raise err

    # write files
    # if stations_cabs_df.size > 0:
    geomerged_file = GEOMERGED_PATH + '/cabs.csv'
    status_1 = file_io.write_csv(df=prev_buffer_ids[0], bucket=REFBASE_BUCKET, filename=geomerged_file)
    # if stations_traffic_df.size > 0:
    geomerged_file = GEOMERGED_PATH + '/traffic.csv'
    status_2 = file_io.write_csv(df=prev_buffer_ids[1], bucket=REFBASE_BUCKET, filename=geomerged_file)

    return status_1 and status_2


if __name__ == '__main__':
    print('performing geographic data-merges for buffer radii %s' % sys.argv[1:])
    #buffer_radii = sys.argv[1:]
    min_radius: float = float(sys.argv[1])
    max_radius: float = float(sys.argv[2])
    step: float = float(sys.argv[3])
    buffer_radii: ndarray = arange(min_radius, max_radius, step)
    status: bool = geo_merge(buffer_radii=buffer_radii)

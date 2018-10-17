from typing import Dict, List, Callable, Union, Optional
from pandas import DataFrame, read_csv
from pandas.io.parsers import TextFileReader
import dask.dataframe as dd
from data_tools import task_map
from utils import persistence as ps
from utils import dask
from data_load import tasks as dl_tasks
from geopandas import GeoDataFrame, sjoin
from shapely.geometry import Point
from data_tools import row_operations as row_ops
from data_tools import file_io
from functools import partial
from numpy import int64
from dask.distributed import Client
#from numba import jit


prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def get_cab_months(task_type: str, task: str) -> List[int]:
    cab_type: str = task_type.rsplit('-', 1)[1]
    months: List[int] = []
    task_split = task.split('-')
    if cab_type == 'gcabs':
        quarter = int(task_split[1])
        months = list(range((quarter - 1) * 3 + 1, (quarter - 1) * 3 + 4))
    elif cab_type == 'ycabs':
        months = [int(task_split[1])]
    return months


def get_cab_filenames(task_type: str, task: str) -> List[str]:
    cab_type: str = task_type.rsplit('-', 1)[1]
    task_split = task.split('-')
    year = task_split[0]
    months = get_cab_months(task_type=task_type, task=task)
    files: List[str] = []

    if cab_type == 'gcabs':
        file_suffix = 'green'
    elif cab_type == 'ycabs':
        file_suffix = 'yellow'

    get_filename = lambda month: file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
    files = list(map(get_filename, months))
    return files


def is_cabs_special_case(task_type: str, task: str) -> bool:
    cab_type: str = task_type.rsplit('-', 1)[1]
    task_split = task.split('-')
    year = task_split[0]
    sub_task = int(task_split[1])
    special_case: bool = cab_type in ['gcabs', 'ycabs'] \
                        and \
                        (
                            (
                                    year in ['2016']
                                    and (
                                            (cab_type == 'gcabs' and sub_task > 2)
                                            or (cab_type == 'ycabs' and sub_task > 6)
                                    )
                            )
                            or year in ['2017', '2018']
                        )
    return special_case


def make_gcabs(*args) -> List[str]:
    map: Dict = task_map.task_type_map['cl-gcabs']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_gcabs(*args)


def make_ycabs(*args) -> List[str]:
    map: Dict = task_map.task_type_map['cl-ycabs']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_ycabs(*args)


def make_transit(*args) -> List[str]:
    map: Dict = task_map.task_type_map['cl-transit']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_transit(*args)


def make_traffic(*args) -> List[str]:
    map: Dict = task_map.task_type_map['cl-traffic']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_traffic(*args)


def remove_outliers(df, col):
    intqrange: float = df[col].quantile(0.75) - df[col].quantile(0.25)
    discard = (df[col] < 0) | (df[col] > 3 * intqrange)
    return df.loc[~discard]


def add_cab_zone(df, lon_var: str, lat_var: str, locid_var: str, taxi_zone_df: GeoDataFrame):
    try:
        if (lat_var in df.columns) and (lon_var in df.columns):
            localdf = df[[lon_var, lat_var]].copy()
            localdf[lon_var] = localdf[lon_var].fillna(value=0.)
            localdf[lat_var] = localdf[lat_var].fillna(value=0.)
            geometry: List[Point] = [Point(xy) for xy in zip(localdf[lon_var], localdf[lat_var])]
            crs: Dict[str, str] = {'init': 'epsg:4326'}
            local_gdf: GeoDataFrame = GeoDataFrame(localdf, crs=crs, geometry=geometry)
            del localdf
            local_gdf = sjoin(local_gdf, taxi_zone_df, how='left', op='within')
            print('after spatial join with taxi zones ')
            return local_gdf.LocationID.rename(locid_var)

        else:
            print('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
            raise KeyError('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
    except Exception as err:
        raise err


def fetch_cab_zones() -> GeoDataFrame:
    # load taxi-zone shapefile
    zipname: str = 'taxi_zones.zip'
    filename: str = 'taxi_zones.shp'
    taxi_zone_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=filename,
                                                              zipname=zipname,
                                                              bucket='ref-base')
    return taxi_zone_df


def get_s3_paths_for_cabs(bucket: str, years: List[str]) -> Dict[str, Dict[str, List[str]]]:
    all_files: List[str] = ps.get_all_filenames(bucket=bucket, path='/')
    s3_prefix: str = 's3://' + bucket + '/'
    special: Dict[str, List[str]] = {year: [] for year in years}
    other: Dict[str, List[str]] = {year: [] for year in years}
    for file in all_files:
        # parse format - file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv
        no_ext: str = file.rsplit('.', 1)[0]
        month: int = int(no_ext.rsplit('-', 1)[1])
        no_ext_no_month: str = no_ext.rsplit('-', 1)[0]
        year: str = no_ext_no_month.rsplit('_', 1)[1]
        special_case: bool = (year in ['2016'] and month > 6) \
                             or year in ['2017', '2018']
        if year in years and special_case:
            special[year].append(s3_prefix+file)
        elif year in years:
            other[year].append(s3_prefix+file)
    return {'special': special, 'other': other}


def clean_cabs_at_path(special: bool, s3_in_url: str, s3_out_url: str, s3_options: Dict) -> bool:

    try:
        df = dd.read_parquet(path=s3_in_url,
                             storage_options=s3_options,
                             engine='fastparquet')

        # add cab zones
        if not special:
            print('In data clean tasks for cabs. Field dolocationid not found')
            # fetch cab zones
            taxi_zones_df: GeoDataFrame = fetch_cab_zones()
            df['dolocationid'] = df.map_partitions(partial(add_cab_zone,
                                                           taxi_zone_df=taxi_zones_df,
                                                           lon_var='dolongitude',
                                                           lat_var='dolatitude',
                                                           locid_var='dolocationid'),
                                                   meta=('dolocationid', int64))
            df['pulocationid'] = df.map_partitions(partial(add_cab_zone,
                                                           taxi_zone_df=taxi_zones_df,
                                                           lon_var='pulongitude',
                                                           lat_var='pulatitude',
                                                           locid_var='pulocationid'),
                                                   meta=('pulocationid', int64))

            del taxi_zones_df
        df = df[['pudatetime', 'dodatetime', 'passengers', 'distance', 'dolocationid', 'pulocationid']]
        dd.to_parquet(df=df,
                      path=s3_out_url,
                      engine='fastparquet',
                      compute=True,
                      compression='GZIP',
                      storage_options=s3_options)
        del df

    except Exception as err:
        print('error in clean_cabs_at_path %s' % str(err))
        raise err

    else:
        return True


#@jit(target='gpu')
def perform_cabs_dask(task_type: str, years: List[str]) -> bool:
    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    client: Client = dask.create_dask_client(num_workers=8)
    special_case: bool = False
    normal_case: bool = False
    s3_in_prefix: str = 's3://' + in_bucket + '/'
    try:
        s3_options: Dict = ps.fetch_s3_options()

        for year in years:

            s3_out_url: str = 's3://' + out_bucket + '/' + year + '/'
            s3_in_url: str = s3_in_prefix + year
            if int(year) == 2016:
                special_case = True
                normal_case = True
            elif int(year) > 2016:
                special_case = True
                normal_case = False
            elif int(year) < 2016:
                special_case = False
                normal_case = True

            if special_case:
                clean_cabs_at_path(special=True,
                                   s3_in_url=s3_in_url + '/special/',
                                   s3_out_url=s3_out_url + '/special/',
                                   s3_options=s3_options)

            if normal_case:
                clean_cabs_at_path(special=False,
                                   s3_in_url=s3_in_url + '/normal/',
                                   s3_out_url=s3_out_url + '/normal/',
                                   s3_options=s3_options)

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        client.close()
        raise err

    client.close()

    return True


def perform_transit_dask(task_type: str, years: List[str]) -> bool:
    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    client: Client = dask.create_dask_client(num_workers=8)
    s3_in_prefix: str = 's3://' + in_bucket + '/'
    try:
        s3_options: Dict = ps.fetch_s3_options()

        for year in years:

            s3_out_url: str = 's3://' + out_bucket + '/' + year + '/'
            s3_in_url: str = s3_in_prefix + year

            df = dd.read_parquet(path=s3_in_url,
                                 storage_options=s3_options,
                                 engine='fastparquet')

            df['delex'] = df['exits'].diff()
            df['delent'] = df['entries'].diff()
            df = df.drop(['exits', 'entries'], axis=1)
            df = df.dropna()

            delex_lo_q = df['delex'].quantile(.25)
            delent_lo_q = df['delent'].quantile(.25)
            delex_hi_q = df['delex'].quantile(.75)
            delent_hi_q = df['delent'].quantile(.75)
            delex_iqr = delex_hi_q - delex_lo_q
            delent_iqr = delent_hi_q - delent_lo_q
            discard = (df['delex'] < delex_lo_q - 1.5 * delex_iqr) | \
                      (df['delex'] > delex_hi_q + 1.5 * delex_iqr) | \
                      (df['delent'] < delent_lo_q - 1.5 * delent_iqr) | \
                      (df['delent'] > delent_hi_q + 1.5 * delent_iqr)
            df = df.loc[~discard]

            dd.to_parquet(df=df,
                          path=s3_out_url,
                          engine='fastparquet',
                          compute=True,
                          compression='lz4',
                          storage_options=s3_options)

    except Exception as err:
        print('error in perform_transit %s' % str(err))
        client.close()
        raise err

    client.close()

    return True


def perform_traffic_dask(task_type: str, years: List[str]) -> bool:
    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    client: Client = dask.create_dask_client(num_workers=8)
    s3_in_prefix: str = 's3://' + in_bucket + '/'
    try:
        s3_options: Dict = ps.fetch_s3_options()
        usecols = [1, 2, 4, 5]
        names = ['speed', 'traveltime', 'datetime', 'linkid']

        for year in years:

            s3_out_url: str = 's3://' + out_bucket + '/' + year + '/'
            s3_in_url: str = s3_in_prefix + '*'+year+'.csv'

            df = dd.read_csv(urlpath=s3_in_url,
                             storage_options=s3_options,
                             header=None,
                             usecols=usecols,
                             names=names,
                             parse_dates=['datetime'],
                             date_parser=row_ops.clean_traffic_date,
                             skipinitialspace=True,
                             skip_blank_lines=True,
                             converters={
                                 'speed': row_ops.clean_num,
                                 'traveltime': row_ops.clean_num,
                                 'linkid': row_ops.clean_num
                             },
                             encoding='utf-8'
                             )

            dd.to_parquet(df=df,
                          path=s3_out_url,
                          engine='fastparquet',
                          compute=True,
                          compression='GZIP',
                          storage_options=s3_options)

    except Exception as err:
        print('error in perform_transit %s' % str(err))
        client.close()
        raise err

    client.close()

    return True


#TODO
def perform_traffic(cab_type: str, b_task: bytes) -> bool:
    return True
from typing import Dict, List, Callable, Union, Optional
from pandas import DataFrame, read_csv
from pandas.io.parsers import TextFileReader
import dask.dataframe as dd
from data_tools import task_map
from utils import persistence as ps
from data_load import tasks as dl_tasks
from geopandas import GeoDataFrame, sjoin
from shapely.geometry import Point
from data_tools import row_operations as row_ops
from data_tools import file_io
from functools import partial
from numpy import int64
from dask.distributed import Client
from distributed.deploy.local import LocalCluster
import time
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


def add_cab_zone(df, lon_vars: List[str], lat_vars: List[str], locid_vars: List[str], taxi_zone_df: GeoDataFrame):
    try:
        #if (lat_var in df.columns) and (lon_var in df.columns):
        localdf = df[lon_vars + lat_vars].copy()
        local_gdf: GeoDataFrame
        crs: Dict[str, str] = {'init': 'epsg:4326'}
        for i, locid in enumerate(locid_vars):
            localdf[lon_vars[i]] = localdf[lon_vars[i]].fillna(value=0.)
            localdf[lat_vars[i]] = localdf[lat_vars[i]].fillna(value=0.)
            if i==0:
                local_gdf = GeoDataFrame(localdf,
                                         crs=crs,
                                         geometry=[Point(xy) for xy in zip(localdf[lon_vars[i]], localdf[lat_vars[i]])])
            else:
                local_gdf['geometry'] = [Point(xy) for xy in zip(localdf[lon_vars[i]], localdf[lat_vars[i]])]
                local_gdf = local_gdf.set_geometry('geometry')
            local_gdf = sjoin(local_gdf, taxi_zone_df, how='left', op='within')
            local_gdf = local_gdf.rename(columns={'LocationID': locid})
        print('after spatial join with taxi zones ')
        return local_gdf[locid_vars]

        #else:
        #    print('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
        #    raise KeyError('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
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


def perform(task_type: str, b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    files: List[str] = []

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    cols: Dict[str, str] = task_type_map['cols']
    parse_dates: bool = task_type_map['dates']['parse']
    in_date_cols: List[str]
    out_date_col: str
    dates: Union[bool, Dict[str, List[str]], List[str]]
    date_parser: Optional[Callable]
    rename_cols: Dict[str, str]
    if parse_dates:
        in_date_cols = task_type_map['dates']['in_cols']
        out_date_col = task_type_map['dates']['out_col']
        if len(in_date_cols) > 1:
            dates = {out_date_col: in_date_cols}
            rename_cols = {col: cols[col] for col in cols.keys() if col not in in_date_cols}
        else:
            dates = in_date_cols
            rename_cols = cols
        date_parser = task_type_map['dates']['parser']
    else:
        dates = False
        date_parser = None
        rename_cols = cols
    converters: Dict[str, Callable] = task_type_map['converters']
    dtypes: Dict[str, str] = task_type_map['dtypes']
    index_col: str = task_type_map['index']['col']
    sorted: bool = task_type_map['index']['sorted']
    row_op: Dict[str, Callable] = task_type_map['row_op']

    if task_type in ['cl-gcabs', 'cl-ycabs']:
        files = get_cab_filenames(task_type=task_type, task=task)

    elif task_type == 'cl-transit':
        task_split = task.split('-')
        year = task_split[0]
        month = int(task_split[1])
        file_part1: str = 'turnstile_' + year + prefix_zero(month)
        file_part2: str = ".txt"
        files = [file_part1 + prefix_zero(day) + file_part2 for day in range(1, 32)]

    print('processing files '+str(files))

    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    # determine if task is cabs special case
    cabs_special_case: bool = False
    if task_type in ['cl-gcabs', 'cl-ycabs']:
        cabs_special_case = is_cabs_special_case(task_type=task_type, task=task)

    # fetch cab zones
    taxi_zones_df: GeoDataFrame = GeoDataFrame()
    if task_type in ['cl-gcabs', 'cl-ycabs'] \
        and not cabs_special_case:
        taxi_zones_df = fetch_cab_zones()

    try:
        for file in files:

            try:
                file_obj = s3.open('s3://'+in_bucket+'/'+file, 'r')

            except Exception:
                # skip file not found (transit)
                continue

            # handle change in data format for cab data

            if cabs_special_case:
                if task_type == 'cl-gcabs':
                    usecols = [2, 6, 7]
                    names = ['dodatetime', 'dolocationid', 'passengers']
                else:
                    usecols = [2, 3, 8]
                    names = ['dodatetime', 'passengers', 'dolocationid']

                df = read_csv(file_obj,
                                 header=None,
                                 usecols=usecols,
                                 names=names,
                                 parse_dates=dates,
                                 date_parser=date_parser,
                                 skipinitialspace=True,
                                 skip_blank_lines=True,
                                 low_memory=False,
                                 converters={
                                     'dodatetime': row_ops.clean_cabs_dt,
                                     'passengers': row_ops.clean_num
                                        },
                                 encoding='utf-8'
                                 )

            else:
                df = read_csv(file_obj,
                                   header=0,
                                   usecols= lambda x: x.strip().lower() in list(cols.keys()),
                                   parse_dates=dates,
                                   date_parser=date_parser,
                                   skipinitialspace=True,
                                   skip_blank_lines=True,
                                   low_memory=False,
                                   converters=converters,
                                   encoding='utf-8'
                                   )

            # rename columns
            df.columns = map(str.lower, df.columns)
            df.columns = map(str.strip, df.columns)
            print('before rename '+str(df.columns))

            df = df.rename(columns=rename_cols)
            print('after rename '+str(df.columns))

            # map row-wise operations
            if task_type in ['cl-gcabs', 'cl-ycabs'] and 'dolocationid' not in df.columns:
                print('In data clean tasks for cabs. Field dolocationid not found')
                # df = df.apply(func=row_op['func'], axis=1)
                df['dolocationid'] = add_cab_zone(df, taxi_zones_df)

            if not sorted:
                df = df.set_index(index_col).sort_index().reset_index()
                print('after sort '+str(df.columns))

            # drop na values
            df = df.dropna()

            file_io.write_csv(df=df, bucket=out_bucket, filename=file)
            print('wrote file to output bucket '+str(file))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True


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


def create_dask_client(num_workers: int) -> Client:
    # initialize distributed client
    # while True:
    #    try:
    #       client: Client = Client('dscheduler:8786')
    #    except (TimeoutError, OSError, IOError):
    #        time.sleep(2)
    #        pass
    #    except Exception as err:
    #        raise err
    #    else:
    #        break
    cluster = LocalCluster(n_workers=num_workers, ip='')
    return Client(cluster)


def perform_dask_test() -> bool :
    client = create_dask_client(4)
    time.sleep(3000)
    return True


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
            df['dolocationid', 'pulocationid'] = df.map_partitions(partial(add_cab_zone,
                                                           taxi_zone_df=taxi_zones_df,
                                                           lon_vars=['dolongitude', 'pulongitude'],
                                                           lat_vars=['dolatitude', 'pulatitude'],
                                                           locid_vars=['dolocationid', 'pulocationid']),
                                                           meta={'dolocationid': int64,
                                                                 'pulocationid': int64})

        df = df[['pudatetime', 'dodatetime', 'passengers', 'distance', 'pulocationid', 'dolocationid']]
        #df = df.drop_duplicates()
        df = df.dropna()
        dd.to_parquet(df=df,
                      path=s3_out_url,
                      engine='fastparquet',
                      compute=True,
                      compression='lz4',
                      storage_options=s3_options)

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

    client: Client = create_dask_client(num_workers=8)
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
#                if task_type == 'cl-gcabs':
#                    usecols = [2, 6, 7]
#                    names = ['dodatetime', 'dolocationid', 'passengers']
#                else:
#                    usecols = [2, 3, 8]
#                    names = ['dodatetime', 'passengers', 'dolocationid']
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

    client: Client = create_dask_client(num_workers=8)
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

            quantiles = df[['delex', 'delent']].quantile(q=[.25, .75]).compute()
            iqr = quantiles.diff()[.75]
            discard = (df['delex'] < quantiles['delex'][.25] - 1.5 * iqr['delex']) | \
                      (df['delex'] > quantiles['delex'][.75] + 1.5 * iqr['delex']) | \
                      (df['delent'] < quantiles['delent'][.25] - 1.5 * iqr['delent']) | \
                      (df['delent'] > quantiles['delent'][.75] + 1.5 * iqr['delent'])
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


#TODO
def perform_traffic(cab_type: str, b_task: bytes) -> bool:
    return True
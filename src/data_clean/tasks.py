import sys
from typing import Dict, List, Callable, Union, Optional
from minio import Object
from pandas import DataFrame, read_csv
from pandas.io.parsers import TextFileReader
from data_tools import task_map
from utils import persistence as ps
from functools import reduce
from data_load import tasks as dl_tasks
from s3fs.core import S3FileSystem
from geopandas import GeoDataFrame, read_file, sjoin
from shapely.geometry import Point
from urllib3.response import HTTPResponse
from data_tools import row_operations as row_ops
from data_tools import file_io


prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def is_cabs_special_case(task_type: str, year: str, sub_task: int) -> bool:
    special_case: bool = task_type in ['cl-gcabs', 'cl-ycabs'] \
            and (
            (
                    year in ['2016']
                    and (
                            (task_type == 'cl-gcabs' and sub_task > 2)
                            or (task_type == 'cl-ycabs' and sub_task > 6)
                    )
            )
            or year in ['2017', '2018']
    )
    return special_case


def make_cabs(cab_type: str, *args) -> List[str]:
    task_type: str = ''
    if cab_type == 'green':
        task_type = 'cl-gcabs'
    elif cab_type == 'yellow':
        task_type = 'cl-ycabs'

    if not task_type == '':
        map: Dict = task_map.task_type_map[task_type]
        out_bucket: str = map['out']
        ps.create_bucket(out_bucket)
        if cab_type == 'green':
            return dl_tasks.make_gcabs(*args)
        elif cab_type == 'yellow':
            return dl_tasks.make_ycabs(*args)
        #return dl_tasks.make_cabs(*args)
        else:
            return []
    else:
        return []


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

def add_cab_zone(df: DataFrame, taxi_zone_df: GeoDataFrame) -> DataFrame:

    try:
        if ('dolatitude' in df.columns) and ('dolongitude' in df.columns):
            geometry: List[Point] = [Point(xy) for xy in zip(df['dolongitude'], df['dolatitude'])]
            df = df.drop(['dolatitude', 'dolongitude'], axis=1)
            crs: Dict[str, str] = {'init': 'epsg:4326'}
            geodf: GeoDataFrame = GeoDataFrame(df, crs=crs, geometry=geometry)
            geodf = sjoin(geodf, taxi_zone_df, how='left', op='within')
            print('after spatial join with taxi zones ')
            df = geodf[['dodatetime', 'LocationID', 'passengers']].rename(columns={'LocationID': 'dolocationid'})

            return df
        else:
            print('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
            raise KeyError('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
    except Exception as err:
        raise err

def fetch_cab_zones() -> GeoDataFrame:

    # load taxi-zone shapefile
    #path_prefix: str = '/tmp/'
    #file_obj: Object = ps.get_file(bucket='ref-base', filename=filename, filepath=path_prefix+filename)
    #print('fetched taxi zones shape file %s' % str(file_obj))
    #taxi_zone_df: GeoDataFrame = read_file('/taxi_zones.shp', vfs='zip://'+path_prefix+filename)
    zipname: str = 'taxi_zones.zip'
    filename: str = 'taxi_zones.shp'
    taxi_zone_df: GeoDataFrame = file_io.fetch_geodf_from_zip(filename=filename,
                                                              zipname=zipname,
                                                              bucket='ref-base')
    return taxi_zone_df



def perform(task_type: str, b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    files: List[str] = []
    task_split: List[str]
    year: str

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
    quarter: int
    #bimonth: int
    month: int
    if task_type in ['cl-gcabs', 'cl-ycabs']:
        task_split = task.split('-')
        year = task_split[0]
        if task_type == 'cl-gcabs':
            file_suffix = 'green'
            quarter = int(task_split[1])
            months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
            get_filename = lambda month: file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv'
            files = list(map(get_filename, months(quarter)))
        elif task_type == 'cl-ycabs':
            file_suffix = 'yellow'
            #bimonth = int(task_split[1])
            #months = lambda bimonth: range((bimonth - 1) * 2 + 1, (bimonth - 1) * 2 + 3)
            #get_filename = lambda month: file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
            #files = list(map(get_filename, months(bimonth)))
            month = int(task_split[1])
            files = [file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv']



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
        cabs_special_case = is_cabs_special_case(task_type=task_type, year=year, sub_task=quarter if task_type == 'cl-gcabs' else month)

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
            #encoding: str = find_encoding(file_obj)
            #print('file encoding is '+encoding)

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
                df = add_cab_zone(df, taxi_zones_df)

            if not sorted:
                df = df.set_index(index_col).sort_index().reset_index()
                print('after sort '+str(df.columns))



            # specific processing for transit
            #if task_type == 'cl-transit':
                #df = remove_outliers(df, col='DELEXITS')

            # drop na values
            df = df.dropna()


            # save in out bucket
            #s3_out_url: str = 's3://' + out_bucket
            #df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'w'))
            file_io.write_csv(df=df, bucket=out_bucket, filename=file)
            print('wrote file to output bucket '+str(file))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True


def perform_large(task_type: str, b_task: bytes, chunksize: int = 500) -> bool:
    task: str = str(b_task, 'utf-8')
    files: List[str] = []
    task_split: List[str]
    year: str

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
    quarter: int
    #bimonth: int
    month: int
    if task_type in ['cl-gcabs', 'cl-ycabs']:
        task_split = task.split('-')
        year = task_split[0]
        if task_type == 'cl-gcabs':
            file_suffix = 'green'
            quarter = int(task_split[1])
            months = lambda quarter: range((quarter - 1) * 3 + 1, (quarter - 1) * 3 + 4)
            get_filename = lambda month: file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
            files = list(map(get_filename, months(quarter)))
        elif task_type == 'cl-ycabs':
            file_suffix = 'yellow'
            #bimonth = int(task_split[1])
            #months = lambda bimonth: range((bimonth - 1) * 2 + 1, (bimonth - 1) * 2 + 3)
            #get_filename = lambda month: file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
            #files = list(map(get_filename, months(bimonth)))
            month = int(task_split[1])
            files = [file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv']



    elif task_type == 'cl-transit':
        task_split = task.split('-')
        year = task_split[0]
        month = int(task_split[1])
        file_part1: str = 'turnstile_' + year + prefix_zero(month)
        file_part2: str = ".txt"
        files = [file_part1 + prefix_zero(day) + file_part2 for day in range(1, 32)]

    print('processing files ' + str(files))

    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    # determine if task is cabs special case
    cabs_special_case: bool = False
    if task_type in ['cl-gcabs', 'cl-ycabs']:
        cabs_special_case = is_cabs_special_case(task_type=task_type, year=year,
                                                 sub_task=quarter if task_type == 'cl-gcabs' else month)

    # fetch cab zones
    taxi_zones_df: GeoDataFrame = GeoDataFrame()
    if task_type in ['cl-gcabs', 'cl-ycabs'] \
        and not cabs_special_case:
        taxi_zones_df = fetch_cab_zones()

    try:
        for file in files:

            try:
                file_obj = s3.open('s3://' + in_bucket + '/' + file, 'r')

            except Exception:
                # skip file not found (transit)
                continue
            # encoding: str = find_encoding(file_obj)
            # print('file encoding is '+encoding)
            tf_reader: TextFileReader
            # handle change in data format for cab data
            if cabs_special_case:
                if task_type == 'cl-gcabs':
                    usecols = [2, 6, 7]
                    names = ['dodatetime', 'dolocationid', 'passengers']
                else:
                    usecols = [2, 3, 8]
                    names = ['dodatetime', 'passengers', 'dolocationid']

                tf_reader = read_csv(file_obj,
                                 header=None,
                                 usecols=usecols,
                                 names=names,
                                 parse_dates=dates,
                                 date_parser=date_parser,
                                 skipinitialspace=True,
                                 skip_blank_lines=True,
                                 chunksize=chunksize,
                                 converters={
                                     'dodatetime': row_ops.clean_cabs_dt,
                                     'passengers': row_ops.clean_num
                                 },
                                 encoding='utf-8'
                                 )

            else:
                tf_reader = read_csv(file_obj,
                                 header=0,
                                 usecols=lambda x: x.strip().lower() in list(cols.keys()),
                                 parse_dates=dates,
                                 date_parser=date_parser,
                                 skipinitialspace=True,
                                 skip_blank_lines=True,
                                 chunksize=chunksize,
                                 converters=converters,
                                 encoding='utf-8'
                                 )
            s3_out_url: str = 's3://' + out_bucket

            for chunk in tf_reader:
                df: DataFrame = DataFrame(chunk)
                # rename columns
                df.columns = map(str.lower, df.columns)
                df.columns = map(str.strip, df.columns)
                print('before rename ' + str(df.columns))

                df = df.rename(columns=rename_cols)
                print('after rename ' + str(df.columns))

                # map row-wise operations
                if task_type in ['cl-gcabs', 'cl-ycabs'] and 'dolocationid' not in df.columns:
                    print('In data clean tasks for cabs. Field dolocationid not found')
                    # df = df.apply(func=row_op['func'], axis=1)
                    df = add_cab_zone(df, taxi_zones_df)

                if not sorted:
                    df = df.set_index(index_col).sort_index().reset_index()
                    print('after sort ' + str(df.columns))

                # specific processing for transit
                # if task_type == 'cl-transit':
                # df = remove_outliers(df, col='DELEXITS')

                # drop na values
                df = df.dropna()

                # save in out bucket
                # s3_out_url: str = 's3://' + out_bucket
                df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'a'))
                # file_io.write_csv(df=df, bucket=out_bucket, filename=file)
                print('appended cleaned chunk to file in output bucket ' + str(file))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True

#TODO
def perform_traffic(cab_type: str, b_task: bytes) -> bool:
    return True
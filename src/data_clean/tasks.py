import sys
from typing import Dict, List, Callable, Union, Optional
import pandas as pd
from data_tools import task_map
from utils import persistence as ps
from functools import reduce
from data_load import tasks as dl_tasks
from s3fs.core import S3FileSystem
from geopandas import GeoDataFrame, read_file, sjoin
from shapely.geometry import Point
from urllib3.response import HTTPResponse


prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def make_cabs(cab_type: str,*args) -> List[str]:
    task_type: str = ''
    if cab_type == 'green':
        task_type = 'cl-gcabs'
    elif cab_type == 'yellow':
        task_type = 'cl-ycabs'

    if not task_type == '':
        map: Dict = task_map.task_type_map[task_type]
        out_bucket: str = map['out']
        ps.create_bucket(out_bucket)
        return dl_tasks.make_cabs(*args)
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

def add_cab_zone(df) -> pd.DataFrame:

    if 'dolocationid' in df.columns:
        print('In data clean tasks for cabs. Field dolocationid already exists')
        return df
    else:
        print('In data clean tasks for cabs. Field dolocationid not found')
        try:
            if ('dolatitude' in df.columns) and ('dolongitude' in df.columns):
                # load taxi-zone shapefile
                taxi_zone_files: List[str] = ['taxi_zones.shp', 'taxi_zones.shx', 'taxi_zones.dbf', 'taxi_zones.shp.xml', 'taxi_zones.sbx', 'taxi_zones.sbn', 'taxi_zones.prj']
                path_prefix: str = '/tmp/'
                for file in taxi_zone_files:
                    ps.get_file(bucket='others', filename=file, filepath=path_prefix+file)
                    print('fetched taxi zones shape file %s' % file)
                taxi_zone_df: GeoDataFrame = read_file(path_prefix+taxi_zone_files[0])
                print('taxi zones GeoDF %s' % str(taxi_zone_df.head(1)))
                geometry: List[Point] = [Point(xy) for xy in zip(df.dolatitude, df.dolongitude)]
                df = df.drop(['dolatitude', 'dolongitude'], axis=1)
                crs: Dict[str, str] = {'init': 'epsg:4326'}
                geodf: GeoDataFrame = GeoDataFrame(df, crs=crs, geometry=geometry)
                print('converted df to GeoDF %s' % str(geodf.head(1)))
                geodf = sjoin(geodf, taxi_zone_df, how='inner', op='within')
                print('after spatial join with taxi zones %s' % str(geodf.head(1)))
                df = geodf[['dodatetime', 'LocationID', 'passengers']].rename({'LocationID':'dolocationid'})
                print('converted back to dataframe %s' % str(df.head(1)))
                return df
            else:
                print('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
                raise KeyError('Data clean tasks for cabs - fields dolocationid, dolatitude, dolongitude not found')
        except Exception as err:
            raise err


def perform(task_type: str, b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    files: List[str] = []
    task_split: List[str]
    year: str
    if task_type in ['cl-gcabs', 'cl-ycabs']:
        if task_type == 'cl-gcabs':
            file_suffix = 'green'
        elif task_type == 'cl-ycabs':
            file_suffix = 'yellow'

        task_split = task.split('-')
        year = task_split[0]
        quarter: int = int(task_split[1])
        months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
        get_filename = lambda month: file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv'
        files = list(map(get_filename, months(quarter)))

    elif task_type == 'cl-transit':
        task_split = task.split('-')
        year = task_split[0]
        month: int = int(task_split[1])
        file_part1: str = 'turnstile_' + year + prefix_zero(month)
        file_part2: str = ".txt"
        files = [file_part1 + prefix_zero(day) + file_part2 for day in range(1, 32)]

    print('processing files '+str(files))

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    cols: Dict[str, str] = task_type_map['cols']
    parse_dates: bool = task_type_map['dates']['parse']
    in_date_cols: List[str]
    out_date_col: str
    dates: Union[bool, Dict[str, List[str]]]
    date_parser: Optional[Callable]
    rename_cols: Dict[str, str]
    if parse_dates:
        in_date_cols = task_type_map['dates']['in_cols']
        out_date_col = task_type_map['dates']['out_col']
        dates = {out_date_col: in_date_cols}
        date_parser = task_type_map['dates']['parser']
        rename_cols = {col: cols[col] for col in cols.keys() if col not in in_date_cols}
    else:
        dates = False
        date_parser = None
        rename_cols = cols
    converters: Dict[str, Callable] = task_type_map['converters']
    dtypes: Dict[str, str] = task_type_map['dtypes']
    index_col: str = task_type_map['index']['col']
    sorted: bool = task_type_map['index']['sorted']
    row_op: Dict[str, Callable] = task_type_map['row_op']
    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    try:
        for file in files:

            try:
                file_obj = s3.open('s3://'+in_bucket+'/'+file, 'r')

            except Exception:
                # skip file not found (transit)
                continue
            #encoding: str = find_encoding(file_obj)
            #print('file encoding is '+encoding)
            df = pd.read_csv(file_obj,
                               header=0,
                               usecols= lambda x: x.strip().lower() in list(cols.keys()),
                               parse_dates=dates,
                               date_parser=date_parser,
                               skipinitialspace=True,
                               converters=converters,
                               encoding='utf-8'
                               )

            # rename columns
            df.columns = map(str.lower, df.columns)
            df.columns = map(str.strip, df.columns)
            print('before rename '+str(df.columns))

            df = df.rename(columns=rename_cols)
            print('after rename '+str(df.columns))

            if not sorted:
                df = df.set_index(index_col).sort_index().reset_index()
                print('after sort '+str(df.columns))


            #map row-wise operations
            if task_type in ['cl-gcabs','cl-ycabs']:
                #df = df.apply(func=row_op['func'], axis=1)
                df = add_cab_zone(df)


            # specific processing for transit
            #if task_type == 'cl-transit':
                #df = remove_outliers(df, col='DELEXITS')

            # drop na values
            df = df.dropna()


            # save in out bucket
            #s3_out_url: str = 's3://' + out_bucket
            df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'w'))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True



def perform_transit(b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    month: int = int(task_split[1])
    file_part1: str = 'turnstile_' + year + prefix_zero(month)
    file_part2: str = ".txt"
    files: List[str] = [file_part1 + prefix_zero(day) + file_part2 for day in range(1, 32)]
    print('processing files '+str(files))

    task_type_map: Dict = task_map.task_type_map['cl-transit']
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    cols: Dict[str, str] = task_type_map['cols']
    converters: Dict[str, Callable] = task_type_map['converters']
    dtypes: Dict[str, str] = task_type_map['dtypes']
    index_col: str = task_type_map['index']['col']
    sorted: bool = task_type_map['index']['sorted']
    row_op: Callable = task_type_map['row_op']

    try:
        for file in files:
            df = pd.read_table('s3://'+in_bucket+'/'+file,
                               sep=',',
                               header=0,
                               usecols= lambda x: x.lower() in list(cols.keys()),
                               skipinitialspace=True,
                               converters=converters
                               )

            # rename columns
            df = df.rename(columns=cols)

            if not sorted:
                df = df.set_index(index_col).sort_index().reset_index()


            #map row-wise operations
            df = df.apply(func=row_op, axis=1)


            # specific processing for transit
            df = remove_outliers(df, col='EXITS')

            # drop na values
            df = df.dropna()


            # save in out bucket
            #s3_out_url: str = 's3://' + out_bucket
            df.to_csv('s3://'+out_bucket+'/'+file)

    except Exception as err:
        print('error in perform_transit %s' % str(err))
        raise err

    return True


#TODO
def perform_traffic(cab_type: str, b_task: bytes) -> bool:
    return True
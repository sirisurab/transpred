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
                df = add_cab_zone(df, taxi_zones_df)

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


def perform_large(task_type: str, b_task: bytes, chunksize: int = 250000) -> bool:
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

    print('processing files ' + str(files))

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
                file_obj = s3.open('s3://' + in_bucket + '/' + file, 'r')

            except Exception:
                # skip file not found (transit)
                continue

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

            # delete output file
            ps.remove_file(out_bucket, file)

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

                # drop na values
                df = df.dropna()

                # save in out bucket
                df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'a'))
                print('appended cleaned chunk to file in output bucket ' + str(file))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True


def get_s3_glob_for_cabs(bucket: str) -> Dict[str, List[str]]:
    all_files: List[str] = ps.get_all_filenames(bucket=bucket, path='/')
    s3_prefix: str = 's3://' + bucket + '/'
    special: List[str] = []
    other: List[str] = []
    for file in all_files:
        # parse format - file_suffix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv
        no_ext: str = file.rsplit('.', 1)[0]
        month: int = int(no_ext.rsplit('-', 1)[1])
        no_ext_no_month: str = no_ext.rsplit('-', 1)[0]
        year: str = no_ext_no_month.rsplit('_', 1)[1]
        special_case: bool = (year in ['2016'] and month > 6) \
                             or year in ['2017', '2018']
        if special_case:
            special.append(s3_prefix+file)
        else:
            other.append(s3_prefix+file)
    return {'special': special, 'other': other}


def perform_dask(task_type: str) -> bool:
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

    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    try:
        s3_options: Dict = ps.fetch_s3_options()

        if task_type in ['cl-gcabs', 'cl-ycabs']:
            s3_glob_cabs: Dict[str, List[str]] = get_s3_glob_for_cabs(bucket=in_bucket)

            for case in s3_glob_cabs.keys():
                if case == 'special':
                    if task_type == 'cl-gcabs':
                        usecols = [2, 6, 7]
                        names = ['dodatetime', 'dolocationid', 'passengers']
                    else:
                        usecols = [2, 3, 8]
                        names = ['dodatetime', 'passengers', 'dolocationid']

                    df = dd.read_csv(urlpath=s3_glob_cabs[case],
                                     storage_options=s3_options,
                                     header=None,
                                     usecols=usecols,
                                     names=names,
                                     parse_dates=dates,
                                     date_parser=date_parser,
                                     skipinitialspace=True,
                                     skip_blank_lines=True,
                                     converters={
                                         'dodatetime': row_ops.clean_cabs_dt,
                                         'passengers': row_ops.clean_num
                                     },
                                     encoding='utf-8'
                                     )
                else:
                    df = dd.read_csv(urlpath=s3_glob_cabs[case],
                                     storage_options=s3_options,
                                     header=0,
                                     usecols=lambda x: x.strip().lower() in list(cols.keys()),
                                     parse_dates=dates,
                                     date_parser=date_parser,
                                     skipinitialspace=True,
                                     skip_blank_lines=True,
                                     converters=converters,
                                     encoding='utf-8'
                                     )
        else:
            s3_in_url: str = 's3://' + in_bucket + '/*.*'
            df = dd.read_csv(urlpath=s3_in_url,
                             storage_options=s3_options,
                             header=0,
                             usecols=lambda x: x.strip().lower() in list(cols.keys()),
                             parse_dates=dates,
                             date_parser=date_parser,
                             skipinitialspace=True,
                             skip_blank_lines=True,
                             converters=converters,
                             encoding='utf-8'
                             )

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
            # fetch cab zones
            taxi_zones_df: GeoDataFrame = fetch_cab_zones()
            #df = add_cab_zone(df, taxi_zones_df)
            df = df.map_partitions(partial(add_cab_zone, taxi_zone_df=taxi_zones_df),
                                   meta={
                                        'dodatetime': 'datetime64[ns]',
                                        'passengers': 'int64',
                                        'dolocationid': 'object'
                                        })

        #if not sorted:
        #    df = df.set_index(index_col).sort_index().reset_index()
        #    print('after sort ' + str(df.columns))

        # drop na values
        df = df.dropna()

        # save in out bucket
        s3_out_url: str = 's3://'+out_bucket+'/*.csv'
        dd.to_csv(df=df,
                  filename=s3_out_url,
                  name_function=lambda i: task_type.rsplit('-', 1)[1]+'_'+i,
                  storage_options=s3_options)

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True


#TODO
def perform_traffic(cab_type: str, b_task: bytes) -> bool:
    return True
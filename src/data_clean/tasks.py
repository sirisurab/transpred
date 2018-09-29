import sys
from typing import Dict, List, Callable
import pandas as pd
from data_process import tasks
from utils import persistence as ps
from functools import reduce
from data_load import tasks as dl_tasks
from s3fs.core import S3FileSystem

resample_map: Dict = {
                        'filter_by': {
                            'key': 'weekday',
                            'value': 2
                        },
                        'freq': '1M'
                    }
prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def make_cabs(cab_type: str,*args) -> List[str]:
    task_type: str = ''
    if cab_type == 'green':
        task_type = 'cl-gcabs'
    elif cab_type == 'yellow':
        task_type = 'cl-ycabs'

    if not task_type == '':
        map: Dict = tasks.task_type_map[task_type]
        out_bucket: str = map['out']
        ps.create_bucket(out_bucket)
        return dl_tasks.make_cabs(*args)
    else:
        return []


def make_transit(*args) -> List[str]:
    map: Dict = tasks.task_type_map['cl-transit']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_transit(*args)


def make_traffic(*args) -> List[str]:
    map: Dict = tasks.task_type_map['cl-traffic']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_traffic(*args)


def remove_outliers(df, col):
    intqrange: float = df[col].quantile(0.75) - df[col].quantile(0.25)
    discard = (df[col] < 0) | (df[col] > 3 * intqrange)
    return df.loc[~discard]


def perform_cabs(cab_type: str, b_task: bytes) -> bool:
    if cab_type == 'green':
        file_suffix = 'green'
        task_type = 'cl-gcabs'
    elif cab_type == 'yellow':
        file_suffix = 'yellow'
        task_type = 'cl-ycabs'

    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    quarter: int = int(task_split[1])
    months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
    get_filename = lambda month: file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv'
    files: List[str] = list(map(get_filename, months(quarter)))
    print('processing files '+str(files))

    task_type_map: Dict = tasks.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    cols: Dict[str, str] = task_type_map['cols']
    converters: Dict[str, Callable] = task_type_map['converters']
    dtypes: Dict[str, str] = task_type_map['dtypes']
    index_col: str = task_type_map['index']['col']
    sorted: bool = task_type_map['index']['sorted']
    row_op: Callable = task_type_map['row_op']
    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    try:
        for file in files:

            df = pd.read_csv(s3.open('s3://'+in_bucket+'/'+file, 'r'),
                               header=0,
                               sep='\t',
                               usecols= lambda x: x.lower() in list(cols.keys()),
                               skipinitialspace=True,
                               converters=converters,
                               encoding='utf-8-sig'
                               )

            # rename columns
            df.columns = map(str.lower, df.columns)
            df.columns = map(str.strip, df.columns)
            print('before rename '+str(df.columns))

            df = df.rename(columns=cols)
            print('after rename '+str(df.columns))

            if not sorted:
                df = df.set_index(index_col).sort_index().reset_index()
                print('after sort '+str(df.columns))


            #map row-wise operations
            df = df.apply(func=row_op, axis=1).dropna()


            # specific processing for transit
            #if task_type == 'cl-transit':
                #df = remove_outliers(df, col='DELEXITS')

            # drop na values
            #df = df.dropna()


            # save in out bucket
            #s3_out_url: str = 's3://' + out_bucket
            df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'wb'))

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

    task_type_map: Dict = tasks.task_type_map['cl-transit']
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
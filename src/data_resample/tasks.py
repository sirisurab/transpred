from typing import Dict, List, Callable, Union, Optional
import pandas as pd
import dask.dataframe as dd
from data_tools import task_map
from utils import persistence as ps
from functools import reduce, partial
from data_load import tasks as dl_tasks
from data_clean.tasks import get_cab_months, get_cab_filenames, create_dask_client
from toolz.functoolz import compose
from dask.distributed import Client
resample_map: Dict = {
    'filter_by': {
        'key': 'weekday',
        'value': 2
    },
    'freq': '1M'
}


prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def make_gcabs(*args) -> List[str]:
    map: Dict = task_map.task_type_map['rs-gcabs']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_gcabs(*args)


def make_ycabs(*args) -> List[str]:
    map: Dict = task_map.task_type_map['rs-ycabs']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_ycabs(*args)


def make_transit(*args) -> List[str]:
    map: Dict = task_map.task_type_map['rs-transit']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_transit(*args)


def make_traffic(*args) -> List[str]:
    map: Dict = task_map.task_type_map['rs-traffic']
    out_bucket: str = map['out']
    ps.create_bucket(out_bucket)
    return dl_tasks.make_traffic(*args)


def remove_outliers(df, cols: List[str]):
    for col in cols:
        intqrange: float = df[col].quantile(0.75) - df[col].quantile(0.25)
        discard = (df[col] < 0) | (df[col] > 3 * intqrange)
        df = df.loc[~discard]
    return df


def perform(task_type: str, b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    files: List[str] = []

    if task_type in ['rs-gcabs', 'rs-ycabs']:
        files = get_cab_filenames(task_type=task_type, task=task)

    elif task_type == 'rs-transit':
        task_split = task.split('-')
        year = task_split[0]
        month: int = int(task_split[1])
        file_part1: str = 'turnstile_' + year + prefix_zero(month)
        file_part2: str = ".txt"
        files = [file_part1 + prefix_zero(day) + file_part2 for day in range(1, 32)]

    print('processing files ' + str(files))

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    # cols: Dict[str, str] = task_type_map['cols']
    date_cols: List[str] = task_type_map['date_cols']
    diff: Dict = task_type_map['diff']
    group: Dict = task_type_map['group']
    filter_by_key: str = resample_map['filter_by']['key']
    filter_by_val: int = resample_map['filter_by']['value']
    resample_freq: str = resample_map['freq']
    aggr_func: Callable = task_type_map['aggr_func']

    dtypes: Dict[str, str] = task_type_map['dtypes']
    index_col: str = task_type_map['index']['col']

    s3 = ps.get_s3fs_client()
    print('got s3fs client')

    try:

        for file in files:

            try:
                file_obj = s3.open('s3://'+in_bucket+'/'+file, 'r')

            except Exception:
                # skip file not found (transit)
                continue

            df = pd.read_csv(file_obj,
                             header=0,
                             usecols=dtypes.keys(),
                             parse_dates=date_cols,
                             skipinitialspace=True,
                             encoding='utf-8'
                             )
            df = df.set_index(index_col)

            print('after set index ')

            if diff['compute']:
                df[diff['new_cols']] = df[diff['cols']].diff()
                df = df.drop(diff['cols'], axis=1)
                diff_cols: Dict[str, str] = dict(zip(diff['cols'], diff['new_cols']))

            # specific processing for transit
            if task_type == 'rs-transit':
                df = remove_outliers(df, cols=diff['new_cols'])

            # filter
            if filter_by_key == 'weekday':
                df = df.loc[df.index.weekday == filter_by_val]

            if group['compute']:
                grouper_cols = group['by_cols']
            else:
                grouper_cols = []

            # resample using frequency and aggregate function specified
            cols = [col for col in df.columns if col not in grouper_cols + [index_col]]
            df = df.groupby([pd.Grouper(freq=resample_freq)] + grouper_cols)[cols].apply(aggr_func)

            # save in out bucket
            df.to_csv(s3.open('s3://'+out_bucket+'/'+file, 'w'))

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True


def get_s3_glob(bucket: str, years: List[str]) -> Dict[str, List[str]]:
    s3_prefix: str = 's3://' + bucket + '/'
    glob_str: Dict[str, List[str]] = {year:
                                          [s3_prefix+'/'+year+'/'+file
                                           for file in ps.get_all_filenames(bucket=bucket, path='/'+year+'/')]
                                      for year in years}
    return glob_str


def resample_at_path(s3_in_url, s3_out_url, s3_options, group, index_col, out_file_prefix='out'):

    aggr_func: Callable
    filter_by_key: str = resample_map['filter_by']['key']
    filter_by_val: int = resample_map['filter_by']['value']
    resample_freq: str = resample_map['freq']

    df = dd.read_parquet(path=s3_in_url,
                         storage_options=s3_options,
                         engine='fastparquet')

    # filter
    if filter_by_key == 'weekday':
        df = df.loc[df[index_col].dt.weekday == filter_by_val]

    if group['compute']:
        grouper_cols = group['by_cols']
        aggr_func = group['aggr_func']
        meta_cols = group['meta']
        cols = list(meta_cols.keys())
        print('meta_cols %s' % meta_cols)

        # resample using frequency and aggregate function specified
        df = df.groupby([pd.Grouper(key=index_col, freq=resample_freq)] + grouper_cols)[cols]. \
            apply(aggr_func, meta=meta_cols)
        # df = df.resample(resample_freq).sum()
        # print('after resampling')

    print('after grouping and resampling %s' % str(df.shape))

    # save in out bucket
    dd.to_csv(df=df,
              filename=s3_out_url,
              name_function=lambda i: out_file_prefix+'_'+str(i),
              storage_options=s3_options)

    # s3_out_url: str = 's3://' + out_bucket + '/' + year + '/'
    # dd.to_parquet(df=df,
    #              path=s3_out_url,
    #              engine='fastparquet',
    #              compute=True,
    #              compression='lz4',
    #              storage_options=s3_options)
    return


def perform_dask(task_type: str, years: List[str]) -> bool:

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']
    group: Dict = task_type_map['group']
    index_col: str = task_type_map['index']['col']

    s3_options: Dict = ps.fetch_s3_options()

    client: Client = create_dask_client(num_workers=8)

    try:
        for year in years:
            s3_in_url: str = 's3://' + in_bucket + '/'+year+'/'
            s3_out_url: str = 's3://' + out_bucket + '/' + year + '/*.csv'
            path: str = ''
            print('s3 url %s' % s3_in_url)
            if task_type in ['rs-gcabs', 'rs-ycabs']:
                if int(year) >= 2016:
                    path = '/special/'
                elif int(year) < 2016:
                    path = '/normal/'

            resample_at_path(s3_in_url+path,
                             s3_out_url,
                             s3_options,
                             group,
                             index_col)

            if int(year) == 2016:
                resample_at_path(s3_in_url + '/normal/',
                                 s3_out_url,
                                 s3_options,
                                 group,
                                 index_col,
                                 'out2')

    except Exception as err:
        print('error in perform_cabs %s')
        client.close()
        raise err

    client.close()

    return True

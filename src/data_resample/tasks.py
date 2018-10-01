from typing import Dict, List, Callable, Union, Optional
import pandas as pd
import dask.dataframe as dd
from data_tools import task_map
from utils import persistence as ps
from functools import reduce, partial
from data_load import tasks as dl_tasks
from toolz.functoolz import compose

resample_map: Dict = {
    'filter_by': {
        'key': 'weekday',
        'value': 2
    },
    'freq': '1M'
}
prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


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


def remove_outliers(df, cols: List[str]):
    for col in cols:
        intqrange: float = df[col].quantile(0.75) - df[col].quantile(0.25)
        discard = (df[col] < 0) | (df[col] > 3 * intqrange)
        df = df.loc[~discard]
    return df


def perform(task_type: str) -> bool:
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
    s3_options: Dict = ps.fetch_s3_options()

    try:

        s3_in_url: str = 's3://' + in_bucket + '/*.*'
        df = dd.read_csv(urlpath=s3_in_url,
                         storage_options=s3_options,
                         header=0,
                         skipinitialspace=True,
                         parse_dates=date_cols,
                         encoding='utf-8'
                         )

        #df = df.set_index(df[index_col].astype('datetime64[ns]'), sorted=True)
        #dtypes = {col: dtypes[col] for col in dtypes.keys() if col != index_col}

        print('after set index ')

        if diff['compute']:
            df[diff['new_cols']] = df[diff['cols']].diff()
            df = df.drop(diff['cols'], axis=1)
            diff_cols: Dict[str] = dict(zip(diff['cols'], diff['new_cols']))
            dtypes = {col if col not in diff['cols'] else diff_cols[col]: dtypes[col] for col in dtypes.keys()}

        # specific processing for transit
        if task_type == 'rs-transit':
            print('meta before removing outliers is '+str(dtypes))
            df = df.map_partitions(partial(remove_outliers, cols=diff['new_cols']),
                                   meta=dtypes)

        # filter
        if filter_by_key == 'weekday':
            df = df.loc[df[index_col].dt.weekday == filter_by_val]

        if group['compute']:
            grouper_cols = group['by_cols']
            dtypes = {col: dtypes[col] for col in dtypes.keys() if col not in grouper_cols}
        else:
            grouper_cols = []

        # resample using frequency and aggregate function specified
        # df = compose(df.resample(resample_freq), aggr_func)
        dtypes = {col: dtypes[col] for col in dtypes.keys() if col != index_col}
        print('meta before grouping is '+str(dtypes))
        df = df.groupby([pd.Grouper(key=index_col, freq=resample_freq)] +
                        grouper_cols).apply(aggr_func, meta=dtypes)

        #df = df.reset_index()

        # save in out bucket
        s3_out_url: str = 's3://' + out_bucket + '/turnstile-*.csv'
        df.to_csv(s3_out_url)

    except Exception as err:
        print('error in perform_cabs %s' % str(err))
        raise err

    return True

import dask.dataframe as dd
import sys
from typing import Dict, Union, Callable
from utils import persistence as ps
from data_process import row_operations as row_ops
from toolz.functoolz import compose
from functools import partial
from dask.distributed import Client
import numpy as np

resample_map: Dict = {
                        'filter_by': {
                            'key': 'weekday',
                            'value': 2
                        },
                        'freq': '1M'
                    }

# read from task_type from sys argument
task_type_map: Dict = {
                  'cl_gcabs': {
                                'in': 'gcabs',
                                'out': 'cl_gcabs',
                                'cols': {
                                        'Lpep_dropoff_datetime':'dropoff_datetime',
                                        'Passenger_count':'passenger_count',
                                        'Dropoff_longitude':'longitude',
                                        'Dropoff_latitude':'latitude'
                                        },
                                'row_op': row_ops.clean_cabs,
                                'converters': {
                                        'dropoff_datetime': row_ops.cl_cabs_dt,
                                        'passenger_count': row_ops.cl_num,
                                        'longitude': row_ops.cl_num,
                                        'latitude': row_ops.cl_num
                                        },
                                'dtypes': {
                                        'dropoff_datetime': 'datetime64[ns]',
                                        'passenger_count': 'int64',
                                        'longitude': 'float64',
                                        'latitude': 'float64'
                                        },
                                'index': {
                                        'col': 'dropoff_datetime',
                                        'sorted': False
                                        },
                                'diff': {
                                        'compute': False
                                        },
                                'aggr_func': ''
                                },
                  'cl_ycabs': {
                                'in': 'gcabs',
                                'out': 'cl_gcabs',
                                'cols': {
                                        'tpep_dropoff_datetime':'dropoff_datetime',
                                        'passenger_count':'passenger_count',
                                        'dropoff_longitude':'longitude',
                                        'dropoff_latitude':'latitude'
                                        },
                                'row_op': row_ops.clean_cabs,
                                'converters': {
                                        'dropoff_datetime': row_ops.cl_cabs_dt,
                                        'passenger_count': row_ops.cl_num,
                                        'longitude': row_ops.cl_num,
                                        'latitude': row_ops.cl_num
                                        },
                                'dtypes': {
                                        'dropoff_datetime': 'datetime64[ns]',
                                        'passenger_count': 'int64',
                                        'longitude': 'float64',
                                        'latitude': 'float64'
                                        },
                                'index': {
                                        'col': 'dropoff_datetime',
                                        'sorted': False
                                        },
                                'diff': {
                                        'compute': False
                                        },
                                'aggr_func': ''
                                },
                  'cl_transit': {
                                'in': 'transit',
                                'out': 'cl_transit',
                                'cols': {
                                        'tpep_dropoff_datetime':'dropoff_datetime',
                                        'passenger_count':'passenger_count',
                                        'dropoff_longitude':'longitude',
                                        'dropoff_latitude':'latitude'
                                        },
                                'dtypes': {
                                        'dropoff_datetime': 'datetime64[ns]',
                                        'passenger_count': 'int64',
                                        'longitude': 'float64',
                                        'latitude': 'float64'
                                        },
                                'index': {
                                        'col': 'dropoff_datetime',
                                        'sorted': False
                                        },
                                'row_op': row_ops.clean_transit,
                                'diff': {
                                        'compute': True,
                                        'col': 'EXITS',
                                        'new_col': 'DELEXITS'
                                        },
                                'aggr_func': ''
                                },
                  'cl_traffic': {
                                'in': 'gcabs',
                                'out': 'cl_gcabs',
                                'row_op': row_ops.clean_traffic,
                                'aggr_func': ''
                                },
                  }


def remove_outliers(df, col):
    intqrange: float = df[col].quantile(0.75) - df[col].quantile(0.25)
    discard = (df[col] < 0) | (df[col] > 3 * intqrange)
    return df.loc[~discard]


def run_pipeline(task_type: str) -> bool:
    map: Dict = task_type_map[task_type]
    in_bucket: str = map['in']
    out_bucket: str = map['out']
    cols: Dict[str, str] = map['cols']
    converters: Dict[str, Callable] = map['converters']
    dtypes: Dict[str, str] = map['dtypes']
    index_col: str = map['index']['col']
    sorted: bool = map['index']['sorted']
    row_op: Callable = map['row_op']
    diff: Dict = map['diff']
    filter_by_key: str = resample_map['filter_by']['key']
    filter_by_val: int = resample_map['filter_by']['value']
    resample_freq: str = resample_map['freq']
    aggr_func: Callable = map['aggr_func']

    try:

        #client = Client(address='dscheduler:8786')

        s3_in_url: str = 's3://'+in_bucket+'/*.*'
        s3_options: Dict = ps.fetch_s3_options()
        #df = dd.read_table(path=s3_in_url, storage_options=s3_options)
        df = dd.read_table(urlpath='tmp/'+in_bucket+'/*.*',
                           header=0,
                           usecols=lambda x: x.upper() in list(cols.keys()),
                           skipinitialspace=True,
                           converters=converters
                           )

        # rename columns
        df = df.rename(columns=cols)
        df.compute()

        if sorted:
            df = df.map_partitions(lambda pdf: pdf.rename(columns=cols)
                                   .apply(func=row_op, axis=1),
                                   meta=dtypes).compute()
        else:
            df = df.map_partitions(lambda pdf: pdf.rename(columns=cols)
                                   .set_index(index_col).sort().reset_index()
                                   .apply(func=row_op, axis=1),
                                   meta=dtypes).compute()



        # map row-wise operations
        #df = df.map_partitions(lambda pdf: pdf.apply(func=row_op, axis=1), meta=dtypes)

        # diff
        if diff['compute']:
            df[diff['new_col']] = df[diff['col']].diff()

        # specific processing for transit
        if task_type == 'cl_transit':
            df = df.map_partitions(partial(remove_outliers, col='DELEXITS'), meta=dtypes)

        # drop na values
        df = df.dropna()

        # set index (assumes pre-sorted data)
        df = df.set_index(index_col, sorted=True)

        #df.compute()

        # filter
        if filter_by_key == 'weekday':
            df = df.loc[df[index_col].weekday() == filter_by_val]

        # resample using frequency and aggregate function specified
        df = compose(df.resample(resample_freq), aggr_func)

        # save in out bucket
        s3_out_url: str = 's3://' + out_bucket
        # dd.to_parquet(df=df, path=s3_out_url, storage_options=s3_options)
        dd.to_parquet(df=df, path='tmp/'+out_bucket+'/*.*')

    except Exception as err:
        print('error in run_pipeline %s' % str(err))
        raise err

    return True


if __name__ == '__main__':
    task_type: str = sys.argv[1]
    # call pipeline function with task_type
    status: bool = run_pipeline(task_type)
    print('pipeline for task %(task)s executed with status %(status)s' % {'task':task_type, 'status': status})
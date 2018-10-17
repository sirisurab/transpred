import pandas as pd
import dask.dataframe as dd
import sys
from data_tools import task_map, file_io
from typing import List, Dict
from utils import persistence as ps
from utils import dask
from urllib3.response import HTTPResponse
from dask.distributed import Client
from functools import partial


def regroup(task_type: str, years: List[str], resample_freq: str, filter_key: str, filter_val: str) -> bool:
    try:
        # determine in and out buckets
        # and split_by from task type map
        in_bucket: str = task_map.task_type_map[task_type]['in']
        out_bucket: str = task_map.task_type_map[task_type]['out']
        split_by: List[str] = task_map.task_type_map[task_type]['split_by']
        date_cols: List[str] = task_map.task_type_map[task_type]['date_cols']
        dtypes: Dict[str, str] = task_map.task_type_map[task_type]['dtypes']
        print('fetched in out and split_by for task_type %(task)s' % {'task': task_type})

        # read files from in bucket and concat into one df
        filestreams: List[HTTPResponse] = ps.get_all_filestreams(bucket=in_bucket)
        s3_in_url: str = 's3://' + in_bucket + '/'
        s3_in_sub_path: str = '/' + resample_freq + '/' + filter_key+filter_val + '/*'
        df = pd.concat([pd.read_csv(s3_in_url+year+s3_in_sub_path,
                                    encoding='utf-8',
                                    parse_dates=date_cols,
                                    dtype=dtypes) for year in years], ignore_index=True)
        print('read files from in bucket and concat-ted into one df')

        # group by split_by and write each group to a separate file
        #s3: s3fs = ps.get_s3fs_client()
        # create out bucket
        #ps.create_bucket(out_bucket)
        for name, group in df.groupby(split_by):
            filename: str
            if isinstance(name, int):
                filename = str(name)
            elif isinstance(name, float):
                filename = str(int(name))
            else:
                filename = str(name).replace('/', ' ')
            #group.to_csv(s3.open('s3://'+out_bucket+'/'+filename, 'w'))
            file_io.write_csv(df=group, bucket=out_bucket, filename=filename)
            #print('wrote file %(file)s' % {'file': filename})

    except Exception as err:
        print('Error: %(error)s in regrouper for task_type %(task)s' % {'error': err, 'task': task_type})
        raise err

    return True


def write_group_to_csv(group, split_by: str, out_bucket: str) -> int:
    filename: str
    #name: str = str(group[split_by].iloc[0]).lstrip(split_by).strip()
    name: str = group.unstack(split_by)[split_by].iloc[0]
    if isinstance(name, int):
        filename = str(name)
    elif isinstance(name, float):
        filename = str(int(name))
    else:
        filename = str(name).replace('/', ' ')
    #group.to_csv(s3.open('s3://'+out_bucket+'/'+filename, 'w'))
    file_io.write_csv(df=group, bucket=out_bucket, filename=filename)
    print('wrote file %(file)s' % {'file': filename})

    return 1


def regroup_dask(task_type: str, years: List[str], resample_freq: str, filter_key: str, filter_val: str) -> bool:
    try:
        # determine in and out buckets
        # and split_by from task type map
        in_bucket: str = task_map.task_type_map[task_type]['in']
        out_bucket: str = task_map.task_type_map[task_type]['out']
        split_by: List[str] = task_map.task_type_map[task_type]['split_by']
        date_cols: List[str] = task_map.task_type_map[task_type]['date_cols']
        dtypes: Dict = task_map.task_type_map[task_type]['dtypes']
        print('fetched in out and split_by for task_type %(task)s' % {'task': task_type})

        # read files from in bucket and concat into one df
        s3_options: Dict = ps.fetch_s3_options()
        client: Client = dask.create_dask_client(num_workers=8)

        # create out bucket
        ps.create_bucket(out_bucket)

        s3_in_url: str = 's3://' + in_bucket + '/'
        s3_in_sub_path: str = '/' + resample_freq + '/' + filter_key+filter_val + '/*'
        df = dd.concat([dd.read_csv(urlpath=s3_in_url+year+s3_in_sub_path,
                                     storage_options=s3_options,
                                     parse_dates=date_cols,
                                     dtype=dtypes
                                     ) for year in years])

        print('read files from in bucket and concat-ted into one df')
        df.groupby(split_by).apply(partial(write_group_to_csv, split_by=split_by, out_bucket=out_bucket), meta=('int')).compute()

    except Exception as err:
        print('Error: %(error)s in regrouper for task_type %(task)s' % {'error': err, 'task': task_type})
        raise err

    return True


if __name__ == '__main__':
    task_type: str = sys.argv[1]
    resample_freq: str = sys.argv[2]
    filter_key: str = sys.argv[3]
    filter_val: str = sys.argv[4]
    years: List[str] = list(sys.argv[5:])
    print('regrouping for task type %s' % task_type)
    #status: bool = regroup(task_type, years, resample_freq, filter_key, filter_val)
    status: bool = regroup_dask(task_type, years, resample_freq, filter_key, filter_val)
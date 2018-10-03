import pandas as pd
import sys
from data_tools import task_map, file_io
from typing import List, Dict
from utils import persistence as ps
from urllib3.response import HTTPResponse
from s3fs import S3FileSystem as s3fs


def regroup(task_type: str) -> bool:
    try:
        # determine in and out buckets
        # and split_by from task type map
        in_bucket: str = task_map.task_type_map[task_type]['in']
        out_bucket: str = task_map.task_type_map[task_type]['out']
        split_by: List[str] = task_map.task_type_map[task_type]['split_by']
        dtypes: Dict[str, str] = task_map.task_type_map[task_type]['dtypes']
        print('fetched in out and split_by for task_type %(task)s' % {'task': task_type})

        # read files from in bucket and concat into one df
        filestreams: List[HTTPResponse] = ps.get_all_filestreams(bucket=in_bucket)
        df = pd.concat([pd.read_csv(stream, encoding='utf-8', dtype=dtypes) for stream in filestreams], ignore_index=True)
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


if __name__ == '__main__':
    task_type: str = sys.argv[1]
    print('regrouping for task type %s' % task_type)
    status: bool = regroup(task_type)
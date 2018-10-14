from utils import persistence as ps
from data_tools import task_map
import sys
from data_resample import tasks as rs_tasks
from data_clean import tasks as cl_tasks
from data_load import tasks as dl_tasks
from typing import List

if __name__ == '__main__':
    task_type: str = sys.argv[1]
    years: List[str] = sys.argv[2:]
    # call pipeline function with task_type
    status: bool = ps.create_bucket(task_map.task_type_map[task_type]['out'])
    if status:

        task_prefix: str = task_type.split('-', 1)[0]
        if task_prefix == 'rs':
            status = rs_tasks.perform_dask(task_type, years)
        elif task_type in ['cl-gcabs', 'cl-ycabs']:
            status = cl_tasks.perform_cabs_dask(task_type, years)
            #status = cl_tasks.perform_dask_test()
        elif task_type in ['dl-gcabs', 'dl-ycabs']:
            status = dl_tasks.perform_cabs_dask(task_type, years)
        elif task_type == 'dl-transit':
            status = dl_tasks.perform_transit_dask(task_type, years)
        elif task_type == 'dl-traffic':
            status = dl_tasks.perform_traffic_dask(task_type, years)

    print('pipeline for task %(task)s executed with status %(status)s' % {'task': task_type, 'status': status})

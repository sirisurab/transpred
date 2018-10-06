from utils import persistence as ps
from data_tools import task_map
import sys
from data_resample import tasks as rs_tasks
from data_clean import tasks as cl_tasks
from typing import List
from dask.distributed import Client

if __name__ == '__main__':
    task_type: str = sys.argv[1]
    years: List[str] = sys.argv[2:]
    # call pipeline function with task_type
    status: bool = ps.create_bucket(task_map.task_type_map[task_type]['out'])
    if status:
        # initialize distributed client
        client: Client = Client('dscheduler:8786')
        task_prefix: str = task_type.split('-', 1)[0]
        if task_prefix == 'rs':
            status = rs_tasks.perform_dask(task_type)
        elif task_prefix == 'cl':
            status = cl_tasks.perform_dask(task_type, years)

    print('pipeline for task %(task)s executed with status %(status)s' % {'task': task_type, 'status': status})

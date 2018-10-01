from utils import persistence as ps
from data_tools import task_map
import sys
from data_resample import tasks as rs_tasks

if __name__ == '__main__':
    task_type: str = sys.argv[1]
    # call pipeline function with task_type
    status: bool = ps.create_bucket(task_map.task_type_map[task_type]['out'])
    if status:
        status = rs_tasks.perform(task_type)
    print('pipeline for task %(task)s executed with status %(status)s' % {'task':task_type, 'status': status})
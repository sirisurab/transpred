# creates tasks for workers
# splits data to be fetched into parts
# if data to be fetched is a single large file, each task is a chunks/part of this file
# if multiple files, each task is a single file or a manageable group of files
# pushes tasks to task/work queue

# input - type of data to be fetched
# decides queue and size of task based on type of data

from typing import List
from data_load import tasks as dl_tasks
from data_clean import tasks as dc_tasks
from utils import messaging as msg
from functools import partial
import sys
import os
# splits job into tasks
# splitting method depends on task_type
# TODO add messaging error handling
# TODO add make tasks error handling
def create_tasks(task_type: str, *args) -> None:
    # pattern match and dispatch
    # dl_transit -> dl.make_tasks_transit(*args)
    # dl_traffic -> dl.make_tasks_traffic(*args)
    # dl_cabs -> dl.make_tasks_cabs(*args)
    # above functions return task list
    #years_lst: List[str] = years.split()
    tasks: List[str]
    print("dispatching from create tasks for %(task)r %(years)s" % {'task':task_type, 'years':args})
    if task_type == 'dl_transit':
        tasks = dl_tasks.make_transit(*args)
    elif task_type == 'dl_traffic':
        tasks = dl_tasks.make_traffic()
    elif task_type == 'dl_gcabs':
        tasks = dl_tasks.make_cabs(*args)
    elif task_type == 'dl_ycabs':
        tasks = dl_tasks.make_cabs(*args)
    elif task_type == 'cl_transit':
        tasks = dc_tasks.make_transit(*args)
    elif task_type == 'cl_traffic':
        tasks = dc_tasks.make_traffic()
    elif task_type == 'cl_gcabs':
        tasks = dc_tasks.make_cabs('green',*args)
    elif task_type == 'cl_ycabs':
        tasks = dc_tasks.make_cabs('yellow',*args)
    else:
        tasks = []

    # push tasks in task list
    # map msg.push_to_q(task_type+'waiting_q',task)
    print('pushing to waiting q tasks '+str(tasks))
    #push_to_waiting_q = partial(msg.push_to_q, queue=task_type+'waiting_q')
    #map(push_to_waiting_q, tasks)
    msg.push_tasks_to_q(tasks, queue=task_type+'waiting_q')

    return


if __name__=="__main__":
    task_type:str = sys.argv[1]
    #task_type: str = os.environ('DATA')
    #years:str = os.environ('YEARS')
    print('calling create tasks for type '+task_type)
    create_tasks(task_type, sys.argv[2:])







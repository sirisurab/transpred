# performs task
# fetches one task from queue
# and performs the task


from data_load import tasks as dl_tasks
from data_clean import tasks as dc_tasks
from data_resample import tasks as rs_tasks
from utils import messaging as msg
from error_handling import errors
import sys
import time

#TODO add messaging error handling
#TODO add perform task error handling
def perform_task(task_type: str) -> bool:
    # fetch task from waiting queue and push to running queue
    task: bytes = fetch_from_q(task_type)
    while task is None or task == b'':
        print('task queue for '+task_type+' is empty. Waiting to try again')
        time.sleep(2)
        task = fetch_from_q(task_type)

    try:
        # pattern match and dispatch
        # turnstile -> dl.perform_task_transit(task)
        # traffic -> dl.perform_task_traffic(task)
        # cabs -> dl.perform_task_cabs(task)
        print('dispatching from perform tasks')
        status: bool
        if task_type == 'dl-transit':
            status = dl_tasks.perform_transit(task)
        elif task_type == 'dl-traffic':
            status = dl_tasks.perform_traffic(task)
        elif task_type == 'dl-gcabs':
            status = dl_tasks.perform_cabs('green', task)
        elif task_type == 'dl-ycabs':
            status = dl_tasks.perform_cabs('yellow', task)
        elif task_type in ['cl-transit', 'cl-traffic', 'cl-gcabs']:
            status = dc_tasks.perform(task_type, task)
        elif task_type in ['cl-ycabs']:
            status = dc_tasks.perform_large(task_type, task, chunksize=5000)
        elif task_type in ['rs-transit', 'rs-traffic', 'rs-gcabs', 'rs-ycabs']:
            status = rs_tasks.perform(task_type, task)
        else:
            raise errors.TaskTypeError(task_type)

    except errors.TaskTypeError as error:
        error.log()
        raise

    else:
        # remove task from running queue
        msg.del_from_q(task, task_type+'running_q')
        return status


def fetch_from_q(task_type: str) -> bytes:
    return msg.pop_q1_push_q2(task_type+'waiting_q', task_type+'running_q')


if __name__=="__main__":
    #task_type: str = os.environ('DATA')
    task_type: str = sys.argv[1]
    perform_task(task_type)



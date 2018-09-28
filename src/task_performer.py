# performs task
# fetches one task from queue
# and performs the task


from data_load import tasks as dl_tasks
from data_clean import tasks as dc_tasks
from utils import messaging as msg
from error_handling import errors
import sys
import time

#TODO add messaging error handling
#TODO add perform task error handling
def perform_task(task_type: str) -> bool:
    # fetch task from waiting queue and push to running queue
    task: bytes = fetch_from_q(task_type)
    if task is None:
        print('task queue for '+task_type+' is empty. Waiting to try again')
        time.sleep(2)
        fetch_from_q(task_type)
        #return True
    elif task == b'':
        return True
    else:
        try:
            # pattern match and dispatch
            # turnstile -> dl.perform_task_transit(task)
            # traffic -> dl.perform_task_traffic(task)
            # cabs -> dl.perform_task_cabs(task)
            print('dispatching from perform tasks')
            status: bool
            if task_type == 'dl_transit':
                status = dl_tasks.perform_transit(task)
            elif task_type == 'dl_traffic':
                status = dl_tasks.perform_traffic(task)
            elif task_type == 'dl_gcabs':
                status = dl_tasks.perform_cabs('green', task)
            elif task_type == 'dl_ycabs':
                status = dl_tasks.perform_cabs('yellow', task)
            if task_type == 'cl_transit':
                status = dc_tasks.perform_transit(task)
            elif task_type == 'cl_traffic':
                status = dc_tasks.perform_traffic(task)
            elif task_type == 'cl_gcabs':
                status = dc_tasks.perform_cabs('green', task)
            elif task_type == 'cl_ycabs':
                status = dc_tasks.perform_cabs('yellow', task)
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



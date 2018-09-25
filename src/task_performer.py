# performs task
# fetches one task from queue
# and performs the task


from data_load import tasks as dl_tasks
from utils import messaging as msg
from error_handling import errors
import sys
import time

#TODO add messaging error handling
#TODO add perform task error handling
def perform_task(task_type: str) -> bool:
    # fetch task from waiting queue and push to running queue
    task: str = fetch_from_q(task_type)
    if not task or task == '':
        print('task queue for '+task_type+' is empty. Waiting to try again')
        time.sleep(2)
        fetch_from_q(task_type)
        #return True
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
            status = dl_tasks.perform_cabs(task)
        else:
            raise errors.TaskTypeError(task_type)

    except errors.TaskTypeError as error:
        error.log()
        raise

    else:
        # remove task from running queue
        msg.del_from_q(task, task_type+'running_q')
        return status


def fetch_from_q(task_type: str) -> str:
    return msg.pop_q1_push_q2(task_type+'waiting_q', task_type+'running_q')


if __name__=="__main__":
    #task_type: str = os.environ('DATA')
    task_type: str = sys.argv[1]
    perform_task(task_type)



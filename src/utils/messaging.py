import redis


def get_client():
    r = redis.Redis(
        host='redis',
        port='6379'
    )
    print('connected to redis %s' % r.info())
    return r

# push task to queue
def push_to_q(msg: str, queue: str) -> None:
    r = get_client()
    r.lpush(queue, msg)
    print('pushed message '+msg+' to '+queue)
    return


# remove/delete message from queue
def del_from_q(msg: str, queue: str) -> None:
    r = get_client()
    r.lrem(queue, msg, 1)
    print('deleted message '+msg+' from '+queue)
    return


# pops message from queue1 and simultaneously pushes the message to queue2
# returns message
def pop_q1_push_q2(pop_queue: str, push_queue: str) -> str:
    r = get_client()
    print('popped from '+pop_queue+' and pushed to '+push_queue)
    return r.rpoplpush(pop_queue, push_queue)
    #return '2016-1'
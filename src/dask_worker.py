#from distributed import Worker
from distributed import Nanny
from tornado.ioloop import IOLoop
from threading import Thread

#w = Worker('tcp://dscheduler:8786')
w = Nanny('tcp://dscheduler:8786')
w.start()  # choose randomly assigned port

loop = IOLoop.current()
loop.start()
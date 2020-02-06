from scrapy_autoproxy.storage_manager import StorageManager
import threading
import time
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


from IPython import embed
#ap = Autoproxy()


def init_sm1():
    sm1 = StorageManager()
    sm1.redis_mgr.test_set_key('sm1','was here')

def init_sm2():
    sm2 = StorageManager()
    sm2.redis_mgr.test_set_key('sm2','was here')


def daemon_fn():
    thread1 = threading.Thread(name="sm1",target=init_sm1)
    print("starting sm1")
    thread1.start()
    time.sleep(5)
    print("starting sm2")
    thread2 = threading.Thread(name="sm2",target=init_sm2)

daemon = threading.Thread(name='daemon', target=daemon_fn)
daemon.setDaemon = True
daemon.start()

daemon.join()
from scrapy_autoproxy.proxy_manager import ProxyManager
import threading
import time
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from IPython import embed
#ap = Autoproxy()

pm = ProxyManager()
embed()
"""

def init_sm1():
    sm1 = StorageManager()
    

def init_sm2():
    sm2 = StorageManager()
    


def daemon_fn():
    thread1 = threading.Thread(name="sm1",target=init_sm1)
    print("starting sm1")
    thread1.start()
    time.sleep(5)
    print("starting sm2")
    thread2 = threading.Thread(name="sm2",target=init_sm2)
    thread2.start()

    

daemon = threading.Thread(name='daemon', target=daemon_fn)
daemon.setDaemon = True
daemon.start()
"""
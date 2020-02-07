from functools import wraps
import logging
import time
import threading
import traceback

logger = logging.getLogger(__name__)
from scrapy_autoproxy.config import config


def block_if_syncing(func):
    @wraps(func)
    def wrapper(self,*args,**kwargs):
        while self.is_syncing() and not self.is_sync_client():
            logger.info("awaiting sync...")
            time.sleep(1)
        return(func(self,*args,**kwargs))
    return wrapper

def queue_lock(func):
    @wraps(func)
    def wrapper(self,queue,*args,**kwargs):
        lock_key = 'syncing_%s' % queue.domain
        lock = self.redis.lock(lock_key)
        if lock.acquire(lock_key, blocking=True, blocking_timeout=0):
            try:
                func(self,queue,*args,**kwargs)
                lock.release()
            except Exception as e:
                logger.error("An error occurred while performing a queue sync operation.")
                traceback.print_exc()
            finally:
                lock.release()
        else:
            while self.redis.exists(lock_key):
                logging.info("Queue sync operation blocked for queue %s" % queue.domain)
                time.sleep(1)

        
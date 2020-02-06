from functools import wraps
import logging
import time
import threading

logger = logging.getLogger(threading.currentThread().getName())
from scrapy_autoproxy.config import config


def block_if_syncing(func):
    @wraps(func)
    def wrapper(self,*args,**kwargs):
        while self.is_syncing() and not self.is_sync_client():
            logger.info(self.is_syncing())
            logger.info(self.is_sync_client())
            logger.info("awaiting sync")
            time.sleep(5)
        return(func(self,*args,**kwargs))
    return wrapper
from functools import wraps
import logging
import time
import threading

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
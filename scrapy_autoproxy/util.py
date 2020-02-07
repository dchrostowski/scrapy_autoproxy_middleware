from functools import wraps
import logging
import time
import threading
import traceback
from datetime import datetime
import random
import re
from urllib.parse import urlparse


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
        if lock.acquire(blocking=True,blocking_timeout=0):
            try:
                func(self,queue,*args,**kwargs)
            except Exception as e:
                logger.error("An error occurred while performing a queue sync operation.")
                traceback.print_exc()
            finally:
                lock.release()
        else:
            while self.redis.exists(lock_key):
                logging.info("Queue sync operation blocked for queue %s" % queue.domain)
                time.sleep(1)
    return wrapper

def parse_domain(url):
    parsed = urlparse(url)
    domain = re.search(r'([^\.]+\.[^\.]+$)',parsed.netloc).group(1)
    return domain


def weighted_chance(prob):
    prob = prob * 100
    result = random.randint(0,100)
    if result < prob:
        return True
    
    return False

def format_redis_boolean(bool_val):
    if bool_val == True:
        return '1'
    elif bool_val == False:
        return '0'

    else:
        raise Exception("invalid boolean")

def format_redis_timestamp(datetime_object):
    if type(datetime_object) != datetime:
        raise Exception("invalid type while formatting datetime to str")
    return datetime_object.isoformat()

def parse_boolean(val):
    if(val == '1' or val == 1 or val == True):
        return True
    elif(val == '0' or val == 0 or val == False):
        return False
    else:
        raise Exception("Invalid value for proxy object boolean")

def parse_timestamp(timestamp_val):
    if type(timestamp_val) == datetime:
        return timestamp_val

    if type(timestamp_val) == str:
        try:
            return datetime.strptime(timestamp_val, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
           return  datetime.strptime(timestamp_val, "%Y-%m-%dT%H:%M:%S")

    else:
        raise Exception("Invalid type for proxy object timestamp")

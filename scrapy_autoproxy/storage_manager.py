from scrapy_autoproxy.redis_manager import RedisManager
from scrapy_autoproxy.postgres_manager import  PostgresManager
from scrapy_autoproxy.util import block_if_syncing
import threading
import time
import logging
logger = logging.getLogger(threading.currentThread().getName())
from functools import wraps
from IPython import embed

# decorator for RedisManager methods


class StorageManager(object):
    def __init__(self):
        self.redis_mgr = RedisManager()
        self.redis = self.redis_mgr.redis
        self.db_mgr = PostgresManager()
        self.sync()

    @block_if_syncing
    def sync_from_db(self):
        logger.info("doing sync...")
        time.sleep(30)
        raise Exception('foobar')

    def is_sync_client(self):
        return self.redis_mgr.is_sync_client()

    def is_syncing(self):
        return self.redis_mgr.is_syncing()

    @block_if_syncing
    def sync(self):
        logger.info("inside sync")
        if len(self.redis.keys()) == 0:
            lock = self.redis.lock('syncing')
            if lock.acquire(blocking=True, blocking_timeout=0):
                try:
                    self.redis.client_setname('syncer')
                    self.redis_mgr.test_set_key('syncer','was here')
                    self.sync_from_db()
                    self.redis.client_setname('')
                except Exception as e:
                    logger.info('EXCEPTION OCCURRED')
                    lock.release()
                    
                    

            logger.info("didn't get lock")



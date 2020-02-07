from scrapy_autoproxy.redis_manager import RedisManager
from scrapy_autoproxy.postgres_manager import  PostgresManager
from scrapy_autoproxy.proxy_objects import Proxy, Detail, Queue
from scrapy_autoproxy.util import block_if_syncing
import threading
import time
import traceback
import logging
logger = logging.getLogger(__name__)
from functools import wraps
from IPython import embed
import uuid



class StorageManager(object):
    def __init__(self):
        self.uuid = str(uuid.uuid4())

        self.redis_mgr = RedisManager(uuid=self.uuid)
        self.redis = self.redis_mgr.redis
        self.is_syncing = self.redis_mgr.is_syncing
        self.is_sync_client = self.redis_mgr.is_sync_client
        
        self.db_mgr = PostgresManager()
        
        self.check_sync()


    @block_if_syncing
    def sync_from_db(self):
        logger.info("initializing seed queues in database...")
        self.db_mgr.init_seed_queues()
        logger.info("initialzing seed details in database...")
        self.db_mgr.init_seed_details()
        queues = self.db_mgr.get_queues()
        proxies = self.db_mgr.get_proxies()

        logger.info("initializing queues in cache...")
        for queue in queues:
            self.redis_mgr.register_queue(queue)
        logger.info("initializing proxies in cache...")
        for proxy in proxies:
            self.redis_mgr.register_proxy(proxy)
            
        self.redis_mgr.reset_temp_proxy_object_ids()
        logging.info("initial sync complete.")



    def check_sync(self):
        if len(self.redis.keys()) == 0:
            lock = self.redis.lock('syncing', thread_local=False)
            if lock.acquire(blocking=True, blocking_timeout=0):
                try:
                    logger.info("StorageManager instance %s has syncing lock" % self.uuid)
                    self.redis.set('syncer',self.uuid)
                    logger.info("Syncing...")
                    self.sync_from_db()
                    self.redis.delete('syncer')
                
                except Exception as e:
                    logger.error("An error ocurred during sync: %s" % e)
                    traceback.print_exc()
                
                finally:    
                    lock.release()

    def initialize_queue(self,queue):
        pass

    def register_detail(self,detail,bypass_db_check):
        pass

            



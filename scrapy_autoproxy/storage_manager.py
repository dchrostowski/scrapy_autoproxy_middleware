from scrapy_autoproxy.redis_manager import RedisManager, RedisDetailQueue
from scrapy_autoproxy.postgres_manager import  PostgresManager
from scrapy_autoproxy.proxy_objects import Proxy, Detail, Queue
from scrapy_autoproxy.util import block_if_syncing, queue_lock
from scrapy_autoproxy.config import config
import threading
import time
import traceback
import logging
logger = logging.getLogger(__name__)
from functools import wraps
from IPython import embed
import uuid
INIT_RDQ_SIZE = config.settings['init_rdq_size']
NEW_QUEUE_PROXY_IDS_PREFIX = 'new_proxy_ids_'
NEW_DETAILS_SET_KEY = 'new_details'


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

    @queue_lock
    def initialize_queue(self,queue):
        if queue.id() is None:
            return
        
        existing_queue_details = self.dbh.get_non_seed_details(queue.queue_id)
        for existing_detail in existing_queue_details:
            detail = self.register_detail(existing_detail,bypass_db_check=True,is_new=False)

    @queue_lock
    def create_new_details(self,queue,count=INIT_RDQ_SIZE):
        fetched_pids_key = "%s%s" % (NEW_QUEUE_PROXY_IDS_PREFIX,queue.domain)
        fetched_pids = list(self.redis_mgr.redis.smembers(fetched_pids_key))
        proxy_ids = self.db_mgr.get_unused_proxy_ids(queue,count,fetched_pids)
        for proxy_id in proxy_ids:
            self.redis_mgr.redis.sadd(fetched_pids_key,proxy_id)
            proxy_key = 'p_%s' % proxy_id
            if not self.redis_mgr.redis.exists(proxy_key):
                raise Exception("Error while trying to create a new detail: proxy key does not exist in redis cache for proxy id %s" % proxy_id)
            
            if self.redis_mgr.redis.exists('d_%s_%s' % (queue.queue_key,proxy_key)):

                continue
            detail_kwargs = {'proxy_id': proxy_id, 'proxy_key': proxy_key, 'queue_id': queue.id(), 'queue_key': queue.queue_key}
            new_detail = Detail(**detail_kwargs)
            self.redis_mgr.redis.sadd(NEW_DETAILS_SET_KEY,new_detail)
            registered_detail = self.register_detail(new_detail,bypass_db_check=True,is_new=True)

    @block_if_syncing
    def register_detail(self,detail,bypass_db_check=False,is_new=False):
        if bypass_db_check and not self.is_syncing:
            logger.warn("bypassing database check...")

        if detail.proxy_key is None or detail.queue_key is None:
            raise Exception('detail object must have a proxy and queue key')
        if not self.redis.exists(detail.proxy_key) or not self.redis.exists(detail.queue_key):
            raise Exception("Unable to locate queue or proxy for detail")

        detail_key = detail.detail_key

        if detail.queue_id is None or detail.proxy_id is None:
            # bypass db check as this must be a new detail (because queue and proxy are not in the database)
            bypass_db_check = True

        if not bypass_db_check:
            db_detail =  self.db_mgr.get_detail_by_queue_and_proxy(queue_id=detail.queue_id, proxy_id=detail.proxy_id)
            if db_detail is not None:
                raise DetailExistsException("Detail already exists in database.  Cannot register detail without deliberately bypassing database check")

        if self.redis.exists(detail.detail_key):
            raise DetailExistsException("Detail is already registered.")
            # return Detail(**self.redis.hgetall(detail_key))
        else:
            self.redis_mgr._register_detail(detail,is_new)
            



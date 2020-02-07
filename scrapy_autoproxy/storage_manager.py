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
            self.redis_mgr.redis.sadd(NEW_DETAILS_SET_KEY,new_detail.detail_key)
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

    def get_queue_count(self,queue):
        return self.redis_mgr.get_queue_count(queue)

    def sync_to_db(self):
        logging.info("STARTING SYNC")
        new_queues = [Queue(**self.redis_mgr.redis.hgetall(q)) for q in self.redis_mgr.redis.keys("qt_*")]
        new_proxies = [Proxy(**self.redis_mgr.redis.hgetall(p)) for p in self.redis_mgr.redis.keys("pt_*")]
        new_detail_keys = set(self.redis_mgr.redis.keys('d_qt*') + self.redis_mgr.redis.keys('d_*pt*'))
        for ndk in new_detail_keys:
            self.redis_mgr.redis.sadd(NEW_DETAILS_SET_KEY, ndk)
        
        new_details = [Detail(**self.redis_mgr.redis.hgetall(d)) for d in list(new_detail_keys)]

        cursor = self.db_mgr.cursor()

        queue_keys_to_id = {}
        proxy_keys_to_id = {}
        for q in new_queues:
            self.db_mgr.insert_queue(q,cursor)
            queue_id = cursor.fetchone()[0]
            queue_keys_to_id[q.queue_key] = queue_id

        for p in new_proxies:
            try:
                self.db_mgr.insert_proxy(p,cursor)
                proxy_id = cursor.fetchone()[0]
                proxy_keys_to_id[p.proxy_key] = proxy_id
            except psycopg2.errors.UniqueViolation as e:

                # existing_proxy = self.db_mgr.get_proxy_by_address_and_port(p.address,p.port)
                proxy_keys_to_id[p.proxy_key] = None


        for d in new_details:
            if d.proxy_id is None:
                new_proxy_id = proxy_keys_to_id[d.proxy_key]
                if new_proxy_id is None:

                    continue
                else:
                    d.proxy_id = new_proxy_id
            if d.queue_id is None:
                d.queue_id = queue_keys_to_id[d.queue_key]
            self.db_mgr.insert_detail(d,cursor)
        

        changed_detail_keys = self.redis_mgr.redis.sdiff('changed_details','new_details')      
        changed_details = [Detail(**self.redis_mgr.redis.hgetall(d)) for d in self.redis_mgr.redis.sdiff('changed_details','new_details')]
        
        for changed in changed_details:
            if(changed.queue_id is None or changed.proxy_id is None):
                raise Exception("Unable to get a queue_id or proxy_id for an existing detail")
            
            self.db_mgr.update_detail(changed)
            


        cursor.close()
        self.redis_mgr.redis.flushall()
        logging.info("SYNC COMPLETE")
        return True
            



import redis
import psycopg2
import sys
import os
import logging
import time
import json
import re
from functools import wraps
from copy import deepcopy
from datetime import datetime, timedelta
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
import traceback

from psycopg2.extras import DictCursor
from psycopg2 import sql
from scrapy_autoproxy.proxy_objects import Proxy, Detail, Queue
from scrapy_autoproxy.util import parse_domain

from scrapy_autoproxy.config import configuration
app_config = lambda config_val: configuration.app_config[config_val]['value']

SEED_QUEUE_ID = app_config('seed_queue')
AGGREGATE_QUEUE_ID = app_config('aggregate_queue')
LIMIT_ACTIVE = app_config('active_proxies_per_queue')
LIMIT_INACTIVE = app_config('inactive_proxies_per_queue')

SEED_QUEUE_DOMAIN = parse_domain(app_config('designated_endpoint'))
AGGREGATE_QUEUE_DOMAIN = 'RESERVED_AGGREGATE_QUEUE'
ACTIVE_LIMIT = app_config('active_proxies_per_queue')
INACTIVE_LIMIT = app_config('inactive_proxies_per_queue')
TEMP_ID_COUNTER = 'temp_id_counter'

BLACKLIST_THRESHOLD = app_config('blacklist_threshold')
MAX_BLACKLIST_COUNT = app_config('max_blacklist_count')
BLACKLIST_TIME = app_config('blacklist_time')
MAX_DB_CONNECT_ATTEMPTS = app_config('max_db_connect_attempts')
DB_CONNECT_ATTEMPT_INTERVAL = app_config("db_connect_attempt_interval")
PROXY_INTERVAL = app_config('proxy_interval')
LAST_USED_CUTOFF = datetime.utcnow() - timedelta(seconds=PROXY_INTERVAL)
NEW_DETAILS_SET_KEY = 'new_details'
CHANGED_DETAILS_SET_KEY = 'changed_details'
INITIAL_SEED_COUNT = app_config('initial_seed_count')
MIN_QUEUE_SIZE = app_config('min_queue_size')
NEW_QUEUE_PROXY_IDS_PREFIX = 'new_proxy_ids_'



class StorageManager(object):
    def __init__(self):
        self.redis_mgr = RedisManager()
        self.db_mgr = PostgresManager()

    def is_syncing(self):
        return self.redis_mgr.is_syncing()

    def new_proxy(self,proxy):
        existing = self.redis_mgr.get_proxy_by_address_and_port(proxy.address,proxy.port)
        if existing is None:
            logging.info("registering new proxy %s" % proxy.urlify())
            new_proxy = self.redis_mgr.register_proxy(proxy)
            new_detail = Detail(proxy_key=new_proxy.proxy_key, queue_id=SEED_QUEUE_ID)
            try:
                self.redis_mgr.register_detail(new_detail)
                self.redis_mgr.redis.sadd(NEW_DETAILS_SET_KEY,new_detail.detail_key)
            except DetailExistsException:
                logging.info("Proxy already exists in cache/db")
                pass
            
        else:
            logging.info("proxy already exists in cache/db")

    def get_seed_queue(self):
        return self.redis_mgr.get_queue_by_id(SEED_QUEUE_ID)
    
    @queue_lock
    def create_new_details(self,queue,count=ACTIVE_LIMIT+INACTIVE_LIMIT):
        logging.info("creating %s new details for domain %s..." % (count, queue.domain))
        fetched_pids_key = "%s%s" % (NEW_QUEUE_PROXY_IDS_PREFIX,queue.domain)
        fetched_pids = list(self.redis_mgr.redis.smembers(fetched_pids_key))
        proxy_ids = self.db_mgr.get_unused_proxy_ids(queue,count,fetched_pids)
        for proxy_id in proxy_ids:
            self.redis_mgr.redis.sadd(fetched_pids_key,proxy_id)
            proxy_key = 'p_%s' % proxy_id
            if not self.redis_mgr.redis.exists(proxy_key):
                raise Exception("Error while trying to create a new detail: proxy key does not exist in redis cache for proxy id %s" % proxy_id)
            
            if self.redis_mgr.redis.exists('d_%s_%s' % (queue.queue_key,proxy_key)):
                logging.warning("Will not create a detail from proxy id %s for %s queue as it already exists" % (proxy_id,queue.domain))
                continue
            detail_kwargs = {'proxy_id': proxy_id, 'proxy_key': proxy_key, 'queue_id': queue.id(), 'queue_key': queue.queue_key}
            new_detail = Detail(**detail_kwargs)
            self.redis_mgr.register_detail(new_detail,bypass_db_check=True)

    
    def sync_to_db(self):
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
                logging.warning("Duplicate proxy, fetch proxy id from database.")
                # existing_proxy = self.db_mgr.get_proxy_by_address_and_port(p.address,p.port)
                proxy_keys_to_id[p.proxy_key] = None


        for d in new_details:
            if d.proxy_id is None:
                new_proxy_id = proxy_keys_to_id[d.proxy_key]
                if new_proxy_id is None:
                    logging.warning("Discarding new detail, as it may already exist.")
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
            
        logging.info("synced redis cache to database, resetting cache.")

        cursor.close()
        self.redis_mgr.redis.flushall()
        return True

        


        
        
        



from scrapy_autoproxy.config import config
from scrapy_autoproxy.util import block_if_syncing
from scrapy_autoproxy.redis_client import Redis
from scrapy_autoproxy.exceptions import RedisDetailQueueEmptyException
from scrapy_autoproxy.proxy_objects import Proxy, Detail, Queue
import logging
import datetime
logger = logging.getLogger(__name__)



TEMP_ID_COUNTER = 'temp_id_counter'
MAX_BLACKLIST_COUNT = int(config.settings['max_blacklist_count'])
BLACKLIST_TIME = int(config.settings['blacklist_time'])





class RedisManager(object):
    def __init__(self,uuid):
        self.redis = Redis.client_factory()
        self.uuid = uuid

    def is_sync_client(self):
        return self.redis.get('syncer') == self.uuid
    
    def is_syncing(self):
        return self.redis.get('syncing') is not None

    @block_if_syncing
    def reset_temp_proxy_object_ids(self):
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'q'),0)
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'p'),0)
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'd'),0)

    @block_if_syncing
    def register_key(self,key,obj):
        redis_key = key
        if obj.id() is None:
            temp_counter_key = "%s_%s" % (TEMP_ID_COUNTER, key)
            redis_key += 't_%s' % self.redis.incr(temp_counter_key)
        else:
            redis_key += '_%s' % obj.id()
        
        self.redis.hmset(redis_key,obj.to_dict(redis_format=True))
        return redis_key

    @block_if_syncing
    def register_queue(self,queue):
        queue_key = self.register_key('q',queue)
        self.redis.hmset(queue_key, {'queue_key': queue_key})
        return Queue(**self.redis.hgetall(queue_key))

    @block_if_syncing
    def register_proxy(self,proxy):
        proxy_key = self.register_key('p',proxy)
        self.redis.hmset(proxy_key, {'proxy_key': proxy_key})
        return Proxy(**self.redis.hgetall(proxy_key))

    @block_if_syncing
    def register_detail(self,detail):
        pass

    def get_queue_by_key(self,queue_key):
        return Queue(**self.redis.hgetall(queue_key))

    def get_proxy(self,proxy_key):
        return Proxy(**self.redis.hgetall(proxy_key))

    def update_detail(self,detail):
        self.redis.hmset(detail.detail_key,detail.to_dict(redis_format=True))
        self.redis.sadd(CHANGED_DETAILS_SET_KEY,detail.detail_key)

    def get_proxy_by_address_and_port(self,address,port):
        proxy_keys = self.redis.keys('p*')
        all_proxy_data = [self.redis.hgetall(pkey) for pkey in proxy_keys]
        for pd in all_proxy_data:
            if str(pd['address']) == str(address):
                if str(pd['port']) == str(port):
                    return Proxy(**pd)


        return None

    def get_all_queue_details(self, queue_key):
        key_match = 'd_%s*' % queue_key
        keys = self.redis.keys(key_match)
        details = [Detail(**self.redis.hgetall(key)) for key in keys]
        return details

    def get_queue_count(self,queue):
        key_match = 'd_%s*' % queue.queue_key
        return len(self.redis.keys(key_match))


        
class RedisDetailQueue(object):
    def __init__(self,queue,active=False):
        self.redis_mgr = RedisManager()
        self.redis = self.redis_mgr.redis
        self.queue = queue
        self.active = active
        active_clause = "active"
        if not active:
            active_clause = "inactive"
        self.redis_key = 'redis_%s_detail_queue_%s' % (active_clause, self.queue.queue_key)


    def _update_blacklist_status(self,detail):
        if detail.blacklisted:
            delta_t = datetime.utcnow() - detail.last_used
            if delta_t.seconds > BLACKLIST_TIME and detail.blacklisted_count < MAX_BLACKLIST_COUNT:
                self.loger.info("unblacklisting detail")
                detail.blacklisted = False
                self.redis_mgr.update_detail(detail)
        
    
    def is_empty(self):
        if not self.redis.exists(self.redis_key):
            return True
        elif self.redis.llen(self.redis_key) == 0:
            return True
        return False

    def enqueue(self,detail):

        self._update_blacklist_status(detail)
        if detail.blacklisted:
            return

        proxy = self.redis_mgr.get_proxy(detail.proxy_key)
        if 'socks' in proxy.protocol:
            return
        
        detail_key = detail.detail_key
        detail_queue_key = self.redis.hget(detail_key,'queue_key')
        
        if detail_queue_key != self.queue.queue_key:
            raise RedisDetailQueueInvalid("No such queue key for detail")
        
        if detail.active != self.active:
            destination_queue = 'active'
            current_queue = "inactive"
            if self.active:
                destination_queue = 'inactive'
                current_queue = "active"

            correct_queue = RedisDetailQueue(self.queue, active=detail.active)
            return correct_queue.enqueue(detail)
            
        self.redis.rpush(self.redis_key,detail_key)



    def dequeue(self):
        if self.is_empty():
            raise RedisDetailQueueEmptyException("No proxies available for queue key %s" % self.queue.queue_key)

        detail = Detail(**self.redis.hgetall(self.redis.lpop(self.redis_key)))
        return detail

    def length(self):
        return self.redis.llen(self.redis_key)

    def clear(self):
        self.redis.delete(self.redis_key)

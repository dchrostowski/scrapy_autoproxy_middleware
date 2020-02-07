from scrapy_autoproxy.util import parse_domain, weighted_chance
from scrapy_autoproxy.storage_manager import StorageManager, RedisDetailQueue
from scrapy_autoproxy.config import config
from scrapy_autoproxy.proxy_objects import ProxyObject
from datetime import datetime
import sys
import logging
logger = logging.getLogger(__name__)
import time

MIN_RDQ_SIZE = int(config.settings['min_rdq_size'])
SEED_FREQUENCY = float(config.settings['seed_frequency'])
PROXY_INTERVAL = int(config.settings['proxy_interval'])
from IPython import embed




class ProxyManager(object):
    def __init__(self):
        self.storage_mgr = StorageManager()

    def get_proxy(self,request_url):
        domain = parse_domain(request_url)
        # get the queue for the request url's domain. If a queue doesn't exist, one will be created.
        queue = self.storage_mgr.redis_mgr.get_queue_by_domain(domain)
        num_details = self.storage_mgr.get_queue_count(queue)
        if num_details == 0:
            self.storage_mgr.initialize_queue(queue=queue)
        
        rdq_active = RedisDetailQueue(queue,active=True)
        rdq_inactive = RedisDetailQueue(queue,active=False)
        num_enqueued = rdq_active.length() + rdq_inactive.length()

        not_enqueued = num_details - num_enqueued
        logging.info("""
        ------------------------------------|
        --------------| Cached total   : %s |
        --------------| Not enqueued   : %s |
        --- ----------| Active RDQ     : %s |
        --------------| Inactive RDQ   : %s |
        -----------------------------------------------|
        """ % (num_details,not_enqueued,rdq_active.length(),rdq_inactive.length()))

        if rdq_inactive.length() < MIN_RDQ_SIZE:
            logger.info("rdq is less than the min rdq size, creating some new details...")
            self.storage_mgr.create_new_details(queue=queue)
            # will return a list of new seed details that have not yet been used for this queue

        elif weighted_chance(SEED_FREQUENCY) and not is_seed:
            self.storage_mgr.create_new_details(queue=queue,count=1)

        use_active = True

        if rdq_active.length() < MIN_RDQ_SIZE:
            use_active=False
            
        
        elif weighted_chance(INACTIVE_PCT):
            use_active = False

        draw_queue = None
        
        if use_active:
            logger.info("using active RDQ")
            draw_queue = rdq_active
        
        else:
            logger.info("using inactive RDQ")
            draw_queue = rdq_inactive
        
        
        detail = draw_queue.dequeue()
        proxy = ProxyObject(detail, StorageManager(), draw_queue)
        
        now = datetime.utcnow()
        elapsed_time = now - proxy.detail.last_used
        if elapsed_time.seconds < PROXY_INTERVAL:
            self.logger.warn("Proxy %s was last used against %s %s seconds ago, using a different proxy." % (proxy.address, domain, elapsed_time.seconds))
            return self.get_proxy(request_url)            
        
        proxy.dispatch()
        return proxy
        
        

    def new_proxy(self,address,port,protocol='http'):
        return self.storage_mgr.new_proxy(address,port,protocol)
        
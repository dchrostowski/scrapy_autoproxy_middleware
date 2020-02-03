class DetailExistsException(Exception):
    pass


# decorator for RedisManager methods
def block_if_syncing(func):
    @wraps(func)
    def wrapper(self,*args,**kwargs):
        while self.is_syncing() and not self.is_sync_client():
            logging.info('awaiting initial sync...')
            time.sleep(5)
        return(func(self,*args,**kwargs))
    return wrapper


# To do - acquire queue sync lock

def queue_lock(func):
    @wraps(func)
    def wrapper(self,*args,**kwargs):
        redis = Redis(**configuration.redis_config)
        queue = kwargs.get('queue',None)
        if queue is None:
            raise Exception("queue_lock function must have a queue kwrargs")
        lock_key = 'syncing_%s' % queue.domain
        lock = redis.lock(lock_key)
        if lock.acquire(blocking=True,blocking_timeout=0):
            logging.info("acquired %s queue lock" % lock_key)
            func(self,*args,**kwargs)
            lock.release()
        
        else:
            while redis.get(lock_key) is not None:
                logging.info("Blocked by %s queue lock waiting for operation to complete." % queue.domain)
                time.sleep(5)
        return
        

    return wrapper


    


class Redis(redis.Redis):
    def __init__(self,*args,**kwargs):
        pool = redis.BlockingConnectionPool(decode_responses=True, *args, **kwargs)
        super().__init__(connection_pool=pool)

class RedisDetailQueueEmpty(Exception):
    pass
class RedisDetailQueueInvalid(Exception):
    pass

class RedisDetailQueue(object):
    def __init__(self,queue,active=True):
        self.redis_mgr = RedisManager()

        self.redis = self.redis_mgr.redis
        self.queue = queue
        self.active = active
        active_clause = "active"
        if not active:
            active_clause = "inactive"
        self.redis_key = 'redis_%s_detail_queue_%s' % (active_clause, self.queue.queue_key)

        self.logger = logging.getLogger('RDQ:%s' % self.queue.domain)

    def reload(self):
        details = self.redis_mgr.get_all_queue_details(self.queue.queue_key)
        self.clear()
        for detail in details:
            if detail.active == self.active:
                self.enqueue(detail)


    def _update_blacklist_status(self,detail):
        if detail.blacklisted:
            last_used = detail.last_used
            now = datetime.utcnow()
            delta_t = now - last_used
            if delta_t.seconds > BLACKLIST_TIME and detail.blacklisted_count < MAX_BLACKLIST_COUNT:
                self.loger.info("unblacklisting detail")
                detail.blacklisted = False
                self.redis_mgr.update_detail(detail)
        
    
    def is_empty(self):
        if not self.redis.exists(self.redis_key):
            return True
        elif self.redis.llen(self.redis_key) == 0:
            return True
        else:
            return False

    def enqueue(self,detail):
        self._update_blacklist_status(detail)
        if detail.blacklisted:
            self.logger.info("detail is blacklisted, will not enqueue")
            return

        proxy = self.redis_mgr.get_proxy(detail.proxy_key)
        if 'socks' in proxy.protocol:
            self.logger.info("not supporting socks proxies right now, will not enqueue proxy %s" % proxy.urlify())
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
            raise RedisDetailQueueEmpty("No proxies available for queue key %s" % self.queue.queue_key)

        detail = Detail(**self.redis.hgetall(self.redis.lpop(self.redis_key)))
        return detail

    def length(self):
        return self.redis.llen(self.redis_key)

    def clear(self):
        self.redis.delete(self.redis_key)
    

class RedisManager(object):
    def __init__(self):
        self.redis = Redis(**configuration.redis_config)
        self.dbh = PostgresManager()

        if len(self.redis.keys()) == 0:
            lock = self.redis.lock('syncing')
            if lock.acquire(blocking=True, blocking_timeout=0):
                self.redis.client_setname('syncer')
                self.sync_from_db()
                self.redis.client_setname('')
                lock.release()

    def is_sync_client(self):
        return self.redis.client_getname() == 'syncer'

    def is_syncing(self):
        return self.redis.get('syncing') is not None



    @block_if_syncing
    def sync_from_db(self):
        logging.info("Syncing proxy data from database to redis...")
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'q'),0)
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'p'),0)
        self.redis.set("%s_%s" % (TEMP_ID_COUNTER, 'd'),0)

        queues = self.dbh.get_queues()
        logging.info("loaded %s queues from database" % len(queues))
        logging.info("queues:")
        logging.info(queues)

        for q in queues:
            self.register_queue(q)

        logging.info("fetching proxies from database...")
        proxies = self.dbh.get_proxies()
        logging.info("got %s proxies from database." % len(proxies))
        logging.info("registering proxies...")
        for p in proxies:
            self.register_proxy(p)
        logging.info("registered proxies.")
        
        logging.info("fetching seed details from database...")
        seed_details = self.dbh.get_seed_details()
        logging.info("got %s seed details from database." % len(seed_details))

        logging.info("registering seed details...")

        seed_queue = self.get_queue_by_id(SEED_QUEUE_ID)
        seed_rdq = RedisDetailQueue(seed_queue)

        for seed_detail in seed_details:
            registered_detail = self.register_detail(seed_detail,bypass_db_check=True)

        logging.info("registered seed details.")
        #logging.info("fetching non-seed details from database...")

        #other_details = []
        """
        for q in queues:
            logging.info("queue id %s" % q.queue_id)
            if q.queue_id != SEED_QUEUE_ID and q.queue_id != AGGREGATE_QUEUE_ID:
                details = self.dbh.get_non_seed_details(queue_id=q.queue_id)
                other_details.extend(details)
        
        
        logging.info("got %s proxy details from database." % len(other_details))
        logging.info("registering proxy details...")
        for d in other_details:
            self.register_detail(d)
        logging.info("registerd proxy details.")
        """
        logging.info("sync complete.")
    
    @block_if_syncing
    def register_object(self,key,obj):
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
        queue_key = self.register_object('q',queue)
        self.redis.hmset(queue_key, {'queue_key': queue_key})

        return Queue(**self.redis.hgetall(queue_key))
    
    @queue_lock
    def initialize_queue(self,queue):
        logging.info("initializing %s queue..." % queue.domain)
        if queue.id() is None:
            logging.warning("Queue does not exist in database yet, skipping database pull...")
            return
        
        existing_queue_details = self.dbh.get_non_seed_details(queue.queue_id)
        for existing_detail in existing_queue_details:
            detail = self.register_detail(existing_detail,bypass_db_check=True)


    @block_if_syncing
    def register_proxy(self,proxy):
        proxy_key = self.register_object('p',proxy)
        self.redis.hmset(proxy_key, {'proxy_key': proxy_key})
        return Proxy(**self.redis.hgetall(proxy_key))
    
    @block_if_syncing
    def register_detail(self,detail,bypass_db_check=False):
        if bypass_db_check and not self.is_syncing:
            logging.warning("WARNING: It is a bad idea to register a detail to the cache without checking that it is in the database first.  I hope you know what you're doing...")
        if detail.proxy_key is None or detail.queue_key is None:
            raise Exception('detail object must have a proxy and queue key')
        if not self.redis.exists(detail.proxy_key) or not self.redis.exists(detail.queue_key):
            raise Exception("Unable to locate queue or proxy for detail")

        detail_key = detail.detail_key

        if detail.queue_id is None or detail.proxy_id is None:
            # bypass db check as this must be a new detail (because queue and proxy are not in the database)
            bypass_db_check = True

        if not bypass_db_check:
            db_detail =  self.dbh.get_detail_by_queue_and_proxy(queue_id=detail.queue_id, proxy_id=detail.proxy_id)
            if db_detail is not None:
                raise DetailExistsException("Detail already exists in database.  Cannot register detail without deliberately bypassing database check")

        if self.redis.exists(detail.detail_key):
            raise DetailExistsException("Detail is already registered.")
            # return Detail(**self.redis.hgetall(detail_key))
        else:
            redis_data = detail.to_dict(redis_format=True)
            self.redis.hmset(detail_key,redis_data)
            if not bypass_db_check:
                self.redis.sadd(NEW_DETAILS_SET_KEY,detail_key)
        
        rdq = RedisDetailQueue(self.get_queue_by_key(detail.queue_key),active=detail.active)
        detail = self.get_detail(detail_key)
        rdq.enqueue(detail)
        return detail

    @block_if_syncing
    def get_detail(self,redis_detail_key):
        return Detail(**self.redis.hgetall(redis_detail_key))

    @block_if_syncing
    def get_all_queues(self):
        return [Queue(**self.redis.hgetall(q)) for q in self.redis.keys('q*')]

    def get_queue_by_domain(self,domain):
        queue_dict = {q.domain: q for q in self.get_all_queues()}
        if domain in queue_dict:
            return queue_dict[domain]
        
        return self.register_queue(Queue(domain=domain))

    @block_if_syncing
    def get_queue_by_id(self,qid):
        lookup_key = "%s_%s" % ('q',qid)
        if not self.redis.exists(lookup_key):
            raise Exception("No such queue with id %s" % qid)
        return Queue(**self.redis.hgetall(lookup_key))

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
        
            


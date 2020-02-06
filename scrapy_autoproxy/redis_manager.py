import redis


from scrapy_autoproxy.config import config
from scrapy_autoproxy.util import block_if_syncing
import logging
logger = logging.getLogger(__name__)


class Redis(redis.Redis):
    def __init__(self,*args,**kwargs):
        pool = redis.BlockingConnectionPool(decode_responses=True, *args, **kwargs)
        super().__init__(connection_pool=pool)

    @staticmethod
    def client_factory(connect_params=config.redis_config):
        return Redis(**connect_params)


class RedisManager(object):
    def __init__(self):
        self.redis = Redis.client_factory()

    def is_sync_client(self):
        return self.redis.client_getname() == 'syncer'
    
    def is_syncing(self):
        return self.redis.keys('syncing') is None

    @block_if_syncing
    def test_set_key(self,name,value):
        logger.info("inside wrapped/blocked fn")
        self.redis.set(name,value)

    def do_nothing(self):
        logger.info("doing nothing")


        


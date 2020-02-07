from scrapy_autoproxy.config import config
import redis

class Redis(redis.Redis):
    def __init__(self,*args,**kwargs):
        pool = redis.BlockingConnectionPool(decode_responses=True, *args, **kwargs)
        super().__init__(connection_pool=pool)

    @staticmethod
    def client_factory(connect_params=config.redis_config):
        return Redis(**connect_params)

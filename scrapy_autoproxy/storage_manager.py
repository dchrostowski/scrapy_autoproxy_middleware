from scrapy_autoproxy.redis_manager import RedisManager
from scrapy_autoproxy.postgres_manager import  PostgresManager



class StorageManager(object):
    def __init__(self):
        self.redis_mgr = RedisManager()
        self.db_mgr = PostgresManager()

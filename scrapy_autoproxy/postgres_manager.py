
from scrapy_autoproxy import config

class PostgresManager(object):
    def __init__(self):
        self.connect_params = None

        
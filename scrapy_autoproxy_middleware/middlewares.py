# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy.exceptions import IgnoreRequest
from scrapy import signals

from scrapy_autoproxy.proxy_manager import ProxyManager
from scrapy_autoproxy.util import parse_domain
import sys
import logging
import twisted

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class AutoproxyDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self,crawler):
        self.proxy_manager = ProxyManager()

        

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.
        
        spider.logger.info("processing request for %s" % request.url)
        if parse_domain(request.url) not in spider.allowed_domains:
            raise IgnoreRequest("Bad domain, ignoring request.")
        
        proxy = self.proxy_mgr.get_proxy(request.url)
        logger.info("using proxy %s" % proxy.urlify())
        request.meta['proxy'] = proxy.urlify()
        request.meta['proxy_obj'] = proxy

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        spider.logger.info("processing response for %s" % request.url)
        proxy = request.meta.get('proxy_obj',None)
        if proxy is None:
            logger.error("No proxy found in request.meta")

        if parse_domain(request.url) not in spider.allowed_domains and parse_domain(response.url) not in spider.allowed_domains:
            logger.info("proxy redirected to a bad domain, marking bad")
            proxy.callback(success=False)
            return response
 
        proxy.callback(success=True)
        return response

    def process_exception(self, request, exception, spider):
        
        # Called when a download handler or a process_request()F
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        spider.logger.info("processing exception for %s" % request.url)

        

        proxy = request.meta.get('proxy_obj',None)

        if proxy is None:
            logger.error("No proxy found in request.meta")
            return request

        
        logger.error("Exception occurred while processing request with proxy %s" % proxy.urlify())
        proxy.callback(success=False)
        return None

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
import random
import requests

class RandomUserAgent(UserAgentMiddleware):
    def __init__(self, user_agent):
        self.user_agent = user_agent

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            user_agent=crawler.settings.get('USER_AGENTS')
        )

    def process_request(self, request, spider):
        agent = random.choice(self.user_agent)
        print('agent = ', agent)
        # request.headers['User-Agent'] = agent
        request.headers.setdefault('User-Agent', agent)

class  RandomHttpProxy(HttpProxyMiddleware):
    def __init__(self, http_proxy):
        self.http_proxy = http_proxy

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            http_proxy= crawler.settings.get('PROXIES')
        )

    def process_request(self, request, spider):
        http_proxy = random.choice(self.http_proxy)
        print('代理IP=',http_proxy)
        request.meta['proxy'] = http_proxy 

'''

class RandomProxy(object):
    """ IP代理 """

    def get_proxy(self):
        r = requests.get('http://127.0.0.1:5000/get')
        return "http://"+ r.text

    def process_request(self, request, spider):
        proxy = self.get_proxy()
        print('代理是：', proxy)
        if proxy:
            request.meta["proxy"] = proxy

'''
class MeituanSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
from lxml import etree
from urllib.parse import urlparse
from redis import StrictRedis, ConnectionPool
from gevent.pool import Pool
import gevent.monkey
import pymongo
import json
import re
import random
import time
import hashlib
import logger

connection_pool = ConnectionPool(host='192.168.31.142', port=6379, db=8)
redis_conn = StrictRedis(connection_pool=connection_pool)
client = pymongo.MongoClient(host='192.168.31.142', port=27017)
db = client['spider']
collection = db['item_detail']
collection.create_index('sku_id', unique=True)
logging = logger.SHlogger().logger


class jd_crawler():
    __slots__ = ['price_url', 'jd_subdomain', 'UAs', 'ulf']

    def __init__(self):
        self.price_url = "https://p.3.cn/prices/mgets?pduid={}&skuIds=J_{}"

        # self.price_backup_url = "https://p.3.cn/prices/get?pduid={}&skuid=J_{}"

        self.jd_subdomain = ['jiadian', 'shouji', 'wt', 'shuma', 'diannao',
                             'bg', 'channel', 'jipiao', 'hotel', 'trip',
                             'ish', 'book', 'e', 'health', 'baby', 'toy',
                             'nong', 'jiu', 'fresh', 'china', 'che', 'list'

                             ]

        self.UAs = ['Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',

                    'Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)',

                    'Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)',

                    'DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)',

                    'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',

                    'Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)',

                    'ia_archiver (+http://www.alexa.com/site/help/webmasters; crawler@alexa.com)', ]

        self.ulf = url_filter()

    def random_UA(self):

        headers = {'user-agent': random.choice(self.UAs)}

        return headers

    def start(self):

        link = 'https://www.jd.com/'
        meta_data = {'cb': 'parse', 'url': link}
        redis_conn.rpush('jd', json.dumps(meta_data))

    def parse(self, data):

        url = data.get('url')
        content = requests.get(url=url, headers=self.random_UA()).text

        response = etree.HTML(content)
        links = response.xpath("//@href")

        for link in links:
            url = urlparse(link)
            if url.netloc:  # 是一个url而不是'javascript:void(0)'
                if not url.scheme:
                    link = 'http:' + link

                subdomain = url.hostname.split('.')[0]

                if subdomain in self.jd_subdomain:
                    if self.ulf.data_filter(link):
                        self.ulf.data_add(link)
                        meta_data = {'cb': 'parse',
                                     'url': link}
                        redis_conn.rpush('jd', json.dumps(meta_data))

                elif subdomain == 'item':

                    if self.ulf.data_filter(link):
                        self.ulf.data_add(link)
                        meta_data = {'cb': 'parse_detail', 'url': link}
                        redis_conn.rpush('jd', json.dumps(meta_data))


    def parse_detail(self, data):
        url = data.get('url')

        content = requests.get(url=url, headers=self.random_UA()).text

        if not content or content == '':
            return
        response = etree.HTML(content)
        item = {}
        item['url'] = url
        item['sku_id'] = re.search('\d+', url, re.S).group()

        item['brand'] = ''.join(response.xpath("id('parameter-brand')/li/a[1]/text()"))

        names = response.xpath('.//div[@class="sku-name"]/text()')

        name = ''.join(str(i) for i in names).strip()

        if name == "":
            name = ''.join(response.xpath('id("name")/h1/text()'))

        item['name'] = name

        try:
            item['shop_name']=response.xpath("//*[contains(@clstag,'dianpu')]/text()")[0]
        except:
            item['shop_name']=None

         
        details = response.xpath('//ul[@class="parameter2 p-parameter-list"]/li/text()')

        if details ==[]:
            try:
                details =self.parse_book(response)

            except IndexError as e:
                if 'item.jd.hk' in url:
                    detail=self.parse_global_shopping(response)
                else:
                    logging.warning('parse failed')

        item['details'] =details
        
        meta_data = {'cb': 'parse_price', 'item': item}

        redis_conn.rpush('jd', json.dumps(meta_data))

    def parse_book(self,response):
        try:
            details=[]
            shop={}
            shop['店铺名称'] =''.join(response.xpath('//ul[@id="parameter2"]/li[1]/a/text()'))
            publisher={}
            publisher['出版社']=''.join(response.xpath('//ul[@id="parameter2"]/li[2]/a/text()'))
            details.append(shop)
            details.append(publisher)
            details+=response.xpath('//ul[@id="parameter2"]/li/text()')[3:]
            return details
        except IndexError as e:
            logging.warning('parse book failed.')
            raise IndexError
    
    def parse_global_shopping(self,response):
        try:

            details = []

            brand = {}

            brand_name = response.xpath(

                "id('item-detail')/div[1]/ul/li[3]/text()")[0]

            brand[brand_name] = response.xpath(

                "id('item-detail')/div[1]/ul/li[3]/a/text()")[0]

            details.append(brand)

            parameters = response.xpath(

                "id('item-detail')/div[1]/ul/li/text()")

            details += parameters[0:2]

            details += parameters[3:]

            return details

        except IndexError as err:
            logging.warn("global shopping parses failed.")
 


    def parse_price(self, data):

        item = data.get('item')
        sku_id = item.get('sku_id')
        url = self.price_url.format(random.randint(1, 100000000), item['sku_id'])
        # logging.info(url)
        response = requests.get(url=url, headers=self.random_UA())
        try:
            price = json.loads(response.text)
            item['price'] = price[0].get('p')

        except KeyError as err:

            if price['error'] == 'pdos_captcha':
                logging.warning("触发验证码")
            redis_conn.rpush('jd', json.dumps(data))

            logging.warning("Price parse error, parse is {}".format(price))

        else:
            try:
                doc = collection.find_one({'sku_id': sku_id})
                if doc is None:
                    logging.info(item)
                    collection.insert_one(item)
                    logging.info('商品插入成功-{}'.format(item['sku_id']))

                else:
                    logging.debug('商品已经存在')
            except Exception as e:
                logging.warning(e)



    def run(self):
        while True:
            data = redis_conn.rpop('jd')

            if data:
                data = json.loads(data)

                
                try:
                    getattr(self, data.get('cb'))(data)
                except Exception as e:
                    logging.warning(e)
                    redis_conn.rpush('jd', json.dumps(data))
                
            else:
                logging.warning('queue is empty')
                time.sleep(5)


class url_filter(object):
    def __init__(self):
        pass

    def data_finger(self, url):
        fp = hashlib.sha1()
        fp.update(url.encode('utf-8'))
        return fp.hexdigest()

    def data_add(self, url):
        fp = self.data_finger(url)
        return redis_conn.sadd('fg', fp)

    def data_filter(self, url):
        fp = self.data_finger(url)
        if redis_conn.sismember('fg', fp):
            return False
        else:
            return True


if __name__ == "__main__":

    gevent.monkey.patch_all()
    crawler = jd_crawler()
    # crawler.start()
    p = Pool(10)
    for i in range(10):
        p.apply_async(crawler.run)
    p.join()

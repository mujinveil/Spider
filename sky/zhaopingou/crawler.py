#!/usr/bin/env python3
# coding=utf-8
import json
import random
import time
from datetime import datetime
from urllib.parse import quote

import config
import gevent.monkey
import requests
from bson.objectid import ObjectId
from gevent.pool import Pool
from lxml import etree
from redis import StrictRedis, ConnectionPool

from utils import cos_client, MAPPING, Mongo, MONGO_URL, SHlogger

connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True)
redis_conn = StrictRedis(connection_pool=connection_pool)
logger = SHlogger().logger
collection = Mongo(MONGO_URL, db_name='spider', coll_name='zhaopingou_resume', unique_index='external_id')
mongo_logger = Mongo(MONGO_URL, db_name='spider', coll_name='spider_logger')


class Crawler(object):

    def __init__(self):

        self.headers = '''
        Accept: multipart/form-data
        Accept-Encoding: gzip, deflate
        Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
        Connection: keep-alive
        Content-Length: 426
        Content-type: application/x-www-form-urlencoded
        '''
        self.proxy = []
        self.request_success = self.request_failed = self.insert_docs = self.invalid_docs = self.repeat_docs = self.update_docs = 0

    def get_proxy(self):
        while True:
            self.proxy = redis_conn.lpop('cookie_ip')
            # logger.info(self.proxy)
            redis_conn.rpush('cookie_ip', self.proxy)
            time.sleep(0.3)

    def send_spider_logger(self):
        datetime_id = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

        mongo_logger.update_one(
            {'channel': 'system.zhaopingou', 'datetime_id': datetime_id},
            {'$inc': {
                'request_success': self.request_success,
                'request_failed': self.request_failed,
                'insert_docs': self.insert_docs,
                'invalid_docs': self.invalid_docs,
                'repeat_docs': self.repeat_docs,
                'update_docs': self.update_docs,
            }},
            upsert=True
        )

        logger.info('send spider logger')
        self.request_success = self.request_failed = self.insert_docs = self.invalid_docs = self.repeat_docs = self.update_docs = 0

    def header_to_dict(self, value):
        if isinstance(value, str):
            headers = {}
            for i in value.split('\n'):
                if i.strip():
                    kv = i.split(':', 1)
                    headers.update({kv[0].strip(): kv[-1].strip()})
            return headers
        else:
            return value

    def get_url(self, keyword):

        url = 'http://qiye.zhaopingou.com/zhaopingou_interface/find_warehouse_by_position_new'
        token = 'AA26D10A4E0626ADFF1BA9F3505803ED'
        for i in range(80):

            keywordencode = quote(keyword)

            post_data = config.post_data1.format(str(i), keywordencode, token)

            try:
                response = requests.post(url, data=post_data, headers=self.header_to_dict(self.headers),
                                         timeout=15).content
                content = json.loads(response.decode('utf-8'))
                warehouseLists = content.get('warehouseList')

            except AttributeError as e:
                logger.debug(e)
                break

            except Exception as e:
                logger.warning(e)

            else:
                for list in warehouseLists:
                    external_id = list['resumeHtmlId']
                    data = str(external_id) + '-' + keyword
                    logger.info(data)
                    redis_conn.rpush('zpg_id', data)

    def get_detail(self, external_id, keyword):

        token = self.proxy.split('-')[0]

        ip = self.proxy.split('-')[1]

        cookie = {'Cookie': 'hrkeepToken={0}'.format(token)}
        # logger.info(cookie)

        proxies = {'http': ip, 'https': ip}

        url = 'http://qiye.zhaopingou.com/zhaopingou_interface/zpg_find_resume_html_details'

        post_data = config.post_data2.format(external_id, token)
        try:
            response = requests.post(url, data=post_data, cookies=cookie, proxies=proxies,
                                     headers=self.header_to_dict(self.headers), timeout=15)
            if response.status_code == 200:
                try:
                    content = json.loads(response.content.decode('utf-8'))
                    html = content.get('jsonHtml')
                    content = etree.HTML(html)
                    self.request_success += 1
                    if self.request_success > 100:
                        self.send_spider_logger()
                        logger.info('已向spider_logger注入日志')
                except:
                    logger.warning('您的操作过于频繁')
                    self.request_failed += 1
                    data = str(external_id) + '-' + keyword
                    redis_conn.rpush('zpg_id', data)
                    return

                resume_updatetime = content.xpath('//span[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', '')
                #logger.info(resume_updatetime)

                doc = collection.find_one(
                    {'external_id': 'zpg_' + str(external_id), 'resume_updatetime': resume_updatetime})

                if doc is None:

                    doc = collection.find_one({'external_id': 'zpg_' + str(external_id)})
                    if doc is None:
                        cos_id = str(ObjectId())
                        doc = {

                            'channel': 'system.zhaopingou',
                            '_created': datetime.utcnow(),
                            'external_id': 'zpg_' + str(external_id),
                            'path': {'name': cos_id + '.html', 'id': cos_id},
                            '_search_options': {'keyword': keyword},
                            '_options': {'update_count': 0},
                            # 'resume_createtime': resume_updatetime,

                        }
                        oid = collection.insert_one(doc, bypass_document_validation=False, session=None)
                        logger.info('insert doc,external_id:{}'.format(external_id))
                        self.insert_docs += 1
                    else:
                        cos_id = doc['path']['id']
                        # logger.info(cos_id)
                        oid = collection.update_one({'external_id': 'zpg_' + str(external_id)},
                                                    {'$set': {'_updated': datetime.utcnow(),
                                                              'resume_updatetime': resume_updatetime, }
                                                     })
                        logger.info('update doc,external_id:{}'.format(external_id))
                        self.update_docs += 1
                    if oid:
                        cos_client.put_object(

                            Bucket='jobs',
                            Body=html.encode('utf-8'),
                            Key=cos_id,
                            Metadata={
                                'x-cos-meta-filename': quote('{}.{}'.format(cos_id, 'html'))},
                            ContentType=MAPPING.get('html'))
                        logger.info('cos注入一条数据')

                else:
                    logger.debug('resume repeated')
                    self.repeat_docs += 1
            else:
                logger.warning(response.status_code)
                self.request_failed += 1

        except Exception as e:
            logger.warning(e)

    def run(self):
        while True:
            try:
                data = redis_conn.lpop('zpg_id')
                external_id = data.split('-')[0]
                keyword = data.split('-')[1]
            except:
                logger.warning('redis is empty')
                time.sleep(1200)
            else:
                if data is None:
                    return
                self.get_detail(external_id, keyword)
                time.sleep(random.randint(2, 3))


def set_ip():
    datas = ['32278C3C914493BD73D494B7453454EE-10.0.0.17:3128', '055D4B61103DE4FEFC3BA9659F0E4008-10.0.40.16:3128',
             '20ED735400872426660636CFEA3C1791-10.0.30.69:3128', '1874E2E0CBA6576A096E31A33D07FD25-10.0.40.13:3128',
             '26A218DBD7C65A16BE7BBCC2A12A5769-10.0.40.8:3128', 'AA26D10A4E0626ADFF1BA9F3505803ED-10.0.0.10:3128',
             'C38BEC4830AC2D79ED3E3DB1F1437DFF-10.0.40.42:3128', 'A308A43AE482A9932BD95433AF2D05A0-10.0.0.17:3128',
             '9BE8BB00E2081BC688B4B7A488AB2212-10.0.0.39:3128']

    for i in datas:
        redis_conn.rpush('cookie_ip', i)


if __name__ == "__main__":

    #set_ip()
    '''
    crawler = Crawler()
    while True:
        keyword = config.keywords.pop()
        print(keyword)
        crawler.get_url(keyword)
    '''

    crawler = Crawler()
    gevent.monkey.patch_all()
    crawler = Crawler()
    p = Pool(8)
    p.apply_async(crawler.get_proxy)
    time.sleep(1)
    for i in range(7):
        p.apply_async(crawler.run)
    p.join()






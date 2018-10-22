#!/usr/bin/env python3
# coding=utf-8
import requests
import time
import json
from datetime import datetime
from bson.objectid import ObjectId
from urllib.parse import quote
from utils import cos_client, MAPPING, SHlogger, Mongo, MONGO_URL
import random
import re
from queue import Queue
from lxml import etree
import config


connection_pool = ConnectionPool(host='localhost', port=6379, db=2, decode_responses=True)

redis_conn = StrictRedis(connection_pool=connection_pool)


def set_ip():
    
    data=token+ip
    
    a=[data]

    for i in a:
        redis_conn.rpush('cookie_ip',i)


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
        self.proxy=[]

    def get_proxy(self):
        while True:
            self.proxy=redis_conn.lpop('cookie_ip')
            print(self.proxy)
            redis_conn.rpush('cookie_ip',self.proxy)
            time.sleep(1)

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

    def get_url(self,keyword):
        
        url = 'http://qiye.zhaopingou.com/zhaopingou_interface/find_warehouse_by_position_new'
        
        token='8C1455D3D6D44355759C54CE9FA22635'
        
        for i in range(80):
            keywordencode=quote(keyword)
            
            post_data = config.post_data1.format(

                    str(i), keywordencode, token)
            try:

                response = requests.post(url, data=post_data, headers=self.header_to_dict(self.headers), timeout=15).content

                content = json.loads(response.decode('utf-8'))

                warehouseLists = content.get('warehouseList')
            
            except AttributeError:
                break
            
            except Exception as e:
                print(e)

            else:

                for list in warehouseLists:

                    external_id = list['resumeHtmlId']

                    data=external_id+keyword

                    redis_conn.rpush('zpg_id',data)






    def get_detail(self,external_id,keyword):

        token=self.proxy.split('-')[0]
        ip=self.proxy.split('-')[1]
        cookie={'Cookie':'hrkeepToken={0}'.format(token)}
        proxies={'http':ip,'https':ip}
        url='http://qiye.zhaopingou.com/zhaopingou_interface/zpg_find_resume_html_details'
        post_data= config.post_data2.format(external_id,token)
        try:
            response = requests.post(url, data=post_data, headers=self.header_to_dict(self.headers), timeout=15)
            if response.status_code == 200:
                try:
                    content = json.loads(response.content.decode('utf-8'))
                    html = content.get('jsonHtml')
                    content = etree.HTML(html)
                except:
                    logger.warning('您的操作过于频繁')
                    tasks_queue.put(data)
                    return

                resume_updatetime = content.xpath('//span[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', '')
                logger.info(resume_updatetime)
                cos_id = str(ObjectId())

                doc = collection.find_one(
                    {'external_id': 'zpg_' + str(external_id), 'resume_updatetime': resume_updatetime})

                if doc is None:
                    doc = collection.find_one({'external_id': 'zpg_' + str(external_id)})
                    if doc is None:

                        doc = {

                            'channel': 'system.zhaopingou',

                            '_created': datetime.utcnow(),

                            'external_id': 'zpg_' + str(external_id),

                            'path': {'name': cos_id + '.html',

                                     'id': cos_id},

                            '_search_options': meta,

                            '_options': {'update_count': 0},

                           #'resume_createtime': resume_updatetime,

                        }
                        oid = collection.insert_one(doc, bypass_document_validation=False, session=None)

                        logger.info('insert doc,external_id:{}'.format(external_id))

                    else:



                        oid = collection.update_one({'external_id': 'zpg_' + str(external_id)},

                                                    {'$set': {'_updated': datetime.utcnow(),

                                                              'resume_updatetime': resume_updatetime,

                                                              }

                                                     })

                        logger.info('update doc,external_id:{}'.format(external_id))

                    if oid:

                        cos_client.put_object(

                            Bucket='jobs',

                            Body=html.encode('utf-8'),

                            Key=cos_id,

                            Metadata={

                                'x-cos-meta-filename': quote('{}.{}'.format(cos_id, 'html'))},

                            ContentType=MAPPING.get('html'))

                else:

                    logger.warning('resume repeated')

            else:

                logger.warning(response.status_code)

        except Exception as e:

            logger.warning(e)

    def run(self):
        while True:
            try:
                data=redis_conn.lpop('zpg_id')
                external_id=data.split('-')[0]
                keyword=data.split('-')[1]
            except:
                logger.warning('redis is empty')
                break

            else:
                self.get_detail(external_id,keyword)
                time.sleep(2)

                
if __name__=="__main__":
    gevent.monkey.patch_all()
    set_ip()
    crawler=Crawler()
    p=Pool(5)
    p.apply_asynic(crawler.get_proxy)
    p.apply_asynic(crawler.get_url)
    for i in range(3):
        p.apply_asynic(crawler.run)
    p.join()


   






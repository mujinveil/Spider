# coding=utf-8
import re
import time
import gevent.monkey
from bello_jtl import Interpreter
import requests
from gevent.pool import Pool
from datetime import datetime
from lxml import etree
from redis import StrictRedis, ConnectionPool
from utils import Mongo, MONGO_URL, SHlogger, cos_client, MAPPING,parse_resume, RESUME_TRANSFORM_MAP
from utils import SHlogger
from utils.tools import header_to_dict
from bson.objectid import ObjectId
from urllib.parse import quote

'''
账号：lk125ijhgu@163.com
密码：KrlEJelc7qTA7gUp
'''

logger = SHlogger().logger

collection = Mongo(MONGO_URL, db_name='temp', coll_name='juxian_resume', unique_index='external_id')

connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=8, decode_responses=True)

redis_conn = StrictRedis(connection_pool=connection_pool)

headers = '''
Accept: */*
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9
Cookie: Hm_lvt_2f46e83a37160121350d426532c94e3a=1541045006; r-c=8a4860444134486688f4ea365dd554e7; .jxtb.auth.UR5660332675=2018/11/1 12:04:58; .jxtb.auth=N6b69h2lEnYuzYRHXkzc9J+SVIJ//erO9EJbIOLln5+phFo6cQQB1em7G6VcuktBPhw4DzwYFPMOovsxWTWEkmDBKmpWB+WK3v4xC4eJ3Co=; Hm_lpvt_2f46e83a37160121350d426532c94e3a=1541052694
Host: www.juxian.com
Proxy-Connection: keep-alive
Referer: http://www.juxian.com/searchcandidate/list?keywords=web&currentIndustries=&employer=&jobTitle=&expectationRegions=&currentRegions=&minAge=&maxAge=&minEducation=0&maxEducation=0&refreshDaysLimit=1024&minWorkYears=&maxWorkYears=&minYearSalary=&maxYearSalary=&expectationIndustries=&resumeLevel=0&page=3
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36
X-Requested-With: XMLHttpRequest
'''


def get_url(keyword):
    for i in range(200,1000):
        logger.info(i)
        url = 'http://www.juxian.com/searchcandidate/PartialList?keywords={1}' \
              '&currentIndustries=&employer=&jobTitle=&expectationRegions=&currentRegions=&' \
              'minAge=&maxAge=&minEducation=0&maxEducation=0&refreshDaysLimit=1024&minWorkYears=' \
              '&maxWorkYears=&minYearSalary=&maxYearSalary=&expectationIndustries=&resumeLevel=0&page={0}'.format(i,keyword)
        response = requests.get(url, headers=header_to_dict(headers)).content.decode('utf-8')

        content = etree.HTML(response)

        nodes = content.xpath('//div[@class="list-content tab-con"]/ul/li/@onclick')

        for i in nodes:
            url = i.split(':')[1]
            external_id = re.search('CB(.*?)keyword', url).group(1).replace('?', '')
            logger.info('external_id-{0} is inserted'.format(external_id))
            data= external_id+'-'+keyword
            redis_conn.rpush('juxian_id', data)


def get_detail():

    headers = '''
    Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
    Accept-Encoding: gzip, deflate
    Accept-Language: zh-CN,zh;q=0.9
    Cookie: Hm_lvt_2f46e83a37160121350d426532c94e3a=1541045006; r-c=8a4860444134486688f4ea365dd554e7; .jxtb.auth.UR5660332675=2018/11/1 12:04:58; .jxtb.auth=N6b69h2lEnYuzYRHXkzc9J+SVIJ//erO9EJbIOLln5+phFo6cQQB1em7G6VcuktBPhw4DzwYFPMOovsxWTWEkmDBKmpWB+WK3v4xC4eJ3Co=; Hm_lpvt_2f46e83a37160121350d426532c94e3a=1541055790
    Host: www.juxian.com
    Proxy-Connection: keep-alive
    Upgrade-Insecure-Requests: 1
    User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36
    '''


    while True:
        try:
            data = redis_conn.lpop('juxian_id')

            external_id =data.split('-')[0]

            keyword = data.split('-')[1]
            url = 'http://www.juxian.com/ent-search/candidate/CB{0}'.format(external_id)


        except Exception as e:
            logger.warning('redis is empty')

            time.sleep(1200)
        else:
            response = requests.post(url, headers=header_to_dict(headers), timeout=15)
            if response.status_code == 200:
                try:
                    content = response.content.decode('utf-8')

                    html =etree.HTML(content)
                except:
                    logger.warning('您的操作过于频繁')

                    return

                resume_update_time = html.xpath('.//span[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', '')
                logger.info(resume_update_time)
                #检查doc 存不存在，更新时间是否是最新的
                doc = collection.find_one(
                    {'external_id': 'juxian_' + str(external_id), 'resume_update_time': resume_update_time})
                if doc is None:
                    doc = collection.find_one({'external_id': 'juxian_' + str(external_id)})
                    if doc is None:
                        cos_id = str(ObjectId())
                        doc = {
                            'channel': 'system.juxian',
                            '_created': datetime.utcnow(),
                            'external_id': 'juxian_' + str(external_id),
                            'path': {'name': cos_id + '.html',
                                     'id': cos_id},
                            '_search_options': {'keyword':keyword},
                            'from_url':'http://www.juxian.com/ent-search/candidate/CB{0}'.format(external_id),
                            '_options': {'update_count': 0},
                            'resume_update_time': resume_update_time,

                        }

                        oid = collection.insert_one(doc, bypass_document_validation=False, session=None)
                        logger.info('insert doc,external_id:{}'.format(external_id))

                    else:
                        cos_id = doc['path']['id']
                        oid = collection.update_one({'external_id': 'juxian_' + str(external_id)},
                                                    {'$set': {'_updated': datetime.utcnow(),
                                                              'resume_update_time': resume_update_time,
                                                              }
                                                     })
                        logger.info('update doc,external_id:{}'.format(external_id))

                    if oid:
                        cos_client.put_object(
                            Bucket='jobs',
                            Body=content.encode('utf-8'),
                            Key=cos_id,
                            Metadata={
                                'x-cos-meta-filename': quote('{}.{}'.format(cos_id, 'html'))},
                            ContentType=MAPPING.get('html'))
                        logger.info('cos注入一条数据')
                else:
                    logger.warning('resume repeated')

            else:
                logger.warning(response.status_code)




def parse_test(external_id):

    doc = collection.find_one({'external_id': external_id})

    cos_id = doc['path']['id']

    try:
        data = cos_client.get_object(Bucket='jobs', Key=cos_id)

    except Exception as e:
        collection.coll.delete_one({'external_id': external_id})
        logger.warning('external_id-{} is deleted'.format(external_id))
        return
    else:
        content = data['Body'].get_raw_stream().data


        if content:
            _resume = parse_resume(content, '{}.html'.format(cos_id))
            result = Interpreter.transformJson(_resume, RESUME_TRANSFORM_MAP)
            print(result)
            '''
            result['_created'] = doc['_created']
            result['path'] = doc['path']
            result['external_id'] = doc['external_id']
            result['channel'] = 'system.juxian'
            #result['_search_options'] = doc['_search_options']
            try:
                result['_updated'] = doc['_updated']
            except:
                pass
            oid = collection_test.replace_one({'channel': 'system.juxian', 'external_id': external_id},
                                              result, upsert=True)
            '''





if __name__ == "__main__":

    gevent.monkey.patch_all()
    p = Pool(4)
    p.apply_async(get_url,args=('大数据',))
    for i in range(3):
        p.apply_async(get_detail)

    p.join()


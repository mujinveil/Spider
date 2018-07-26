
#coding=utf-8
from mongo_redis_mgr import MongoRedisUrlManager
from lxml import etree
import re
import time
import requests
import hashlib
import urllib.request
import json
import threading
headers = {
    #'Host':'www.qichacha.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    #'Cookie': 'UM_distinctid=16121a0f2a63c3-0713b5f898047-7c4f563c-100200-16121a0f2a74bf; zg_did=%7B%22did%22%3A%20%2216121a0f3648ff-0a562c97d5d608-7c4f563c-100200-16121a0f3653b4%22%7D; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201531107991117%2C%22updated%22%3A%201531108542503%2C%22info%22%3A%201530973560631%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22www.baidu.com%22%2C%22cuid%22%3A%20%22908f5b9623aa33e00d7cf0329d147118%22%7D; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1530974766,1530974783,1531098981,1531107992; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1531108522; CNZZDATA1254842228=2122350312-1516684102-https%253A%252F%252Fwww.baidu.com%252F%7C1531095313; hasShow=1; _uab_collina=153110579312886083820397; _umdata=C234BF9D3AFA6FE728D0E08CE6020BA352E98C93A914E1BEC5430A7DA6B66C1AA4648CBA48EFD1E6CD43AD3E795C914C3C92F3C286FAFD270C11F0E51E78AB77; PHPSESSID=ch0f3hn0s9dl2ffcmrlsu2uc87; acw_tc=AQAAACtekxw9KgAA6wA0pK1m2lRdpRg+'
}

j=1
def getcontent():
    global j
    while j<100000:
        try:
            url='http://www.creditbj.gov.cn/xyData/front/creditService/getPageList.shtml?pageNo=%s&keyword=&typeId=19'%str(j)
            data=requests.get(url=url,headers=headers)
            a=(json.loads(data.text))
            db=MongoRedisUrlManager()
            for i in a['hits']['hits']:
                print('开始注入数据',j)
                #print(i['_source'])
                db.db.credit.insert(i['_source'])
        except:
            pass
        j+=1

thread_list=[]
for io in range(5):
    t=threading.Thread(target=getcontent)
    thread_list.append(t)
for t in thread_list:
    t.start()



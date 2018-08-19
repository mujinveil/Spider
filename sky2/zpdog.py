#coding=utf-8
import requests
import time
from gevent.queue import Queue
from gevent.pool import Pool
import json
import pymongo
import datetime
from bson.objectid import ObjectId
from urllib.parse import quote
from utils import cos_client,MAPPING



client=pymongo.MongoClient(host='localhost',port=27017)
db=client['spider']
collection=db['zpdog-resume']
collection.create_index('external_id',unique=True)
tasks_queue=Queue()

class crawl(object):
    def __init__(self):
        self.headers='''
        Accept: multipart/form-data
        Accept-Encoding: gzip, deflate
        Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
        Connection: keep-alive
        Content-Length: 426
        Content-type: application/x-www-form-urlencoded
        Cookie: JSESSIONID=34217FD0D8CD573DF59EAE586D02C4F8; fanwenTime1="2018-08-19 09:31:49"; xiaxifanwenTime1="2018-08-19 09:34:47"; fangWenIp=58.250.255.19; xiaoxiNumber=2; fangWenNumber1=4; rd_apply_lastsession_code=0; hrkeepToken=E85533917BBCE307950B8AED243D7A14; zhaopingou_account=13728955797; zhaopingou_login_callback=/; zhaopingou_select_city=3; JSESSIONID=930F555242E0C6640E1BAE319DBC7257; Hm_lvt_b025367b7ecea68f5a43655f7540e177=1534558674,1534583249,1534642296; zhaopingou_zengsong_cookie_newDay=2018-08-19%3D2; zhaopingou_htm_cookie_register_userName=; zhaopingou_htm_cookie_newDay=2018-08-19; Hm_lpvt_b025367b7ecea68f5a43655f7540e177=1534642623
        Host: qiye.zhaopingou.com
        Origin: http://qiye.zhaopingou.com
        Referer: http://qiye.zhaopingou.com/resume?key=python
        User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36
       '''
        self.timestamp=str(int(time.time()*1000))
    
    def header_to_dict(self,value):
        if isinstance(value, str):
            headers = {}
            for i in value.split('\n'):
                if i.strip():
                    kv = i.split(':', 1)
                    headers.update({kv[0].strip(): kv[-1].strip()})
            return headers
        else:
            return value


    def start(self):
        keywords=['java','python']
        url='http://qiye.zhaopingou.com/zhaopingou_interface/find_warehouse_by_position_new?timestamp={0}'.format(self.timestamp)
        for keyword in keywords:
             for i in range(80):
                 post_data='pageSize={0}&pageNo=25&keyStr={1}&companyName=&schoolName=&keyStrPostion=&postionStr=&startDegrees=-1&endDegress=-1&startAge=0&endAge=0&gender=-1&region=&timeType=-1&startWorkYear=-1&endWorkYear=-1&beginTime=&endTime=&isMember=-1&hopeAdressStr=&cityId=3&updateTime=&tradeId=&startDegreesName=&endDegreesName=&tradeNameStr=&regionName=&isC=0&is211_985_school=0&clientNo=&userToken=E85533917BBCE307950B8AED243D7A14&clientType=2'.format(str(i),keyword)
                 tasks_queue.put({
                 'callback':'parse',
                 'url':url,       
                 'post_data':post_data,
                 'meta':{'keyword':keyword}
                  })


    def parse(self,data):
        url=data.get('url')
        post_data=data.get('post_data')
        meta=data.get('meta')
        response=requests.post(url,data=post_data,headers=header_to_dict(self.headers)).content 
        content=json.loads(response.decode('utf-8'))
        resumeids=content.get['resumeHtmlId']
        for resumeid in resumeids:
            tasks_queue.put({
              'callback':'parse_detail',
              'url':'http://qiye.zhaopingou.com/zhaopingou_interface/zpg_find_resume_html_details?timestamp={0}'.format(self.timestamp),
              'post_data':'resumeHtmlId={0}&keyStr=&keyPositionName=&tradeId=&postionStr=&jobId=0&companyName=&schoolName=&clientNo=&userToken=E85533917BBCE307950B8AED243D7A14&clientType=2'.format(resumeid),
              'external_id':resumeid
              'meta':meta
               })

    def parse_detail(self,data):
        url=data.get('url')
        post_data=data.get('post_data')
        meta=data.get('meta')
        external_id=data.get('external_id')
        doc=collection.find_one({'external_id':external_id})
        if doc is None:
            response=requests.post(url,data=post_data,headers=header_to_dict(self.headers)).content
            content=json.loads(response.decode('utf-8'))
            html=content.get['jsonHtml']
            if html:
                cos_id=str(Objectid())
                doc = {
                    '_created': datetime.utcnow(),
                    'external_id':external_id,
                    'path': {
                        'name': cos_id + '.html',  # cos文件名
                        'id': cos_id,
                    },
                    '_search_options': meta,
                    'from_url': url,
                    }
                oid=collection.insert_one(doc)

                if oid:
                    cos_client.put_object(
                        Bucket='resumes',
                        Body=html,
                        Key=cos_id,
                        Metadata={
                            'x-cos-meta-filename': quote('{}.{}'.format(cos_id, 'html'))},
                        ContentType=MAPPING.get('html')
                    )
    def run(self):
        while True:
            try:
                data = tasks_queue.get(timeout=10)
            except:
                logger.warning('queue is empty')
                break
            else:
                try:
                    getattr(self, data.get('cb'))(data)
                except Exception as e:
                    pass


if __name__ == '__main__':

    POOL_NUM = 1
    crawler = Crawler()
    if POOL_NUM < 2:
        crawler.start()
        #crawler.run()
    else:

        gevent.monkey.patch_all()
        p = Pool(POOL_NUM)

        time.sleep(1)
        p.apply_async(crawler.start)
        for i in range(POOL_NUM):
            p.apply_async(crawler.run)
        p.join()








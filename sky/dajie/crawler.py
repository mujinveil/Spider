# coding=utf-8
import json
import random
import time
from datetime import datetime
from urllib.parse import quote
import schedule
import gevent.monkey
import requests
from bson.objectid import ObjectId
from gevent.pool import Pool
from redis import StrictRedis, ConnectionPool

from utils import Mongo, MONGO_URL, SHlogger, cos_client, MAPPING

collection = Mongo(MONGO_URL, db_name='spider', coll_name='dajie_resume', unique_index='external_id')
connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True)
mongo_logger = Mongo(MONGO_URL, db_name='spider', coll_name='spider_logger')
redis_conn = StrictRedis(connection_pool=connection_pool)
connection_pool2 = ConnectionPool(host='10.0.40.42', port=6379, db=0, decode_responses=True)
redis_proxy = StrictRedis(connection_pool=connection_pool2)
logger = SHlogger().logger
REDISKEY = 'dajie_id'


class Crawler(object):
    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Host': 'job.dajie.com',
            'Referer': 'https: // job.dajie.com / search / talent / condition / list / index?keyword = % E8 % AF % 81 % E5 % 88 % B8 % E4 % BB % A3 % E8 % A1 % A8 &from=top_nav & scrollTop = 0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        self.cookies = [

            {
                'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M6BWaFWZZZLw-qKxjA5eYaZeqBkLftxuoGsIAeuWPC8Mf2YOsDyTyZtDtsvpi4c0pA**'},
            {
                'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M5vSOEJd6wA1jIpxuLb9tKUlU9AiqwyOKqEqc9FL_C6n3bcxL1_klOwCCoyDpY9u2A**'},

            {
                'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=Mtj7xKJJOum0k_tf2MLNKCM0QtZmuH-5y7aZTh4EUXTh3hao2qcTH7FNDj3nwCc0;'}

        ]

        self.cities = [110000, 310000, 120000, 500000, 340000, 350000, 620000, 440000, 450000, 520000, 460000, 130000,
                       410000, 230000, 430000, 220000, 320000, 360000, 210000, 150000, 640000, 630000, 370000, 140000,
                       610000, 510000, 540000, 650000, 530000, 330000, 800000, 810000, 999999]

        self.return_code = self.request_success = self.request_failed = self.insert_docs = self.invalid_docs = self.repeat_docs = self.update_docs = 0
        self.proxy_list = ['socks5://127.0.0.1:9999']

    def proxy(self):
        while True:
            proxy_list = redis_proxy.hkeys('proxy')
            if proxy_list:
                self.proxy_list = proxy_list
            time.sleep(2)

    def get_proxy(self):
        if self.proxy_list:
            random_proxy = random.choice(self.proxy_list)
            return random_proxy

    def send_spider_logger(self):
        datetime_id = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

        mongo_logger.update_one(
            {'channel': 'system.dajie', 'datetime_id': datetime_id},
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

    def get_url(self, keyword):
        for city in self.cities:
            for i in range(100):
                verify_num = 0
                keywordencode = quote(keyword)
                logger.info('关键词是{0},已经爬取城市{1}的第{2}页'.format(keyword, city, i))
                url = 'https://job.dajie.com/search/talent/ajax/getresultlist?ajax=1&page={0}&conditionId=' \
                      '&schoolType=&special=&schoolIds=&homeCity=&overseas=&majorIds=&language=&graduateYears' \
                      '=&searchRange=&keyWordMode=0&keyword={1}&isIncludeKeyword=&corpName=' \
                      '&isCompanyNameIncludeKeyword=&positionFunction=&industry=&city={2}&workExperience' \
                      '=&minDegree=&maxDegree=&minAge=&maxAge=&gender=&salary=&updateType=&filterNoImage=0' \
                      '&filterReaded=0&filterDownResume=0&sortMode=2'.format(
                    str(i + 1), keywordencode, city)

                proxy = self.get_proxy()

                proxies = {'http': 'http://' + proxy,
                           'https': 'http://' + proxy}

                cookie = self.cookies[0]  # random.choice(self.cookies)

                try:
                    response = requests.get(url=url, headers=self.headers, cookies=cookie, proxies=proxies,
                                            timeout=5).content

                    content = json.loads(response.decode('utf-8'))
                    print(content)

                    if content['result'] == 1:
                        time.sleep(60)
                    try:
                        datas = content.get('data').get('list')
                        print(datas)
                        if len(datas) == 0:
                            break
                        for data in datas:
                            # uid = data['uid']
                            encryptedId = data['encryptedId']
                            external_id = encryptedId + '-' + keyword
                            logger.info(external_id)
                            redis_conn.rpush(REDISKEY, external_id)
                    except AttributeError:
                        verify_num += 1
                    if verify_num >= 10:
                        return

                    time.sleep(random.randint(2, 3))
                except json.decoder.JSONDecodeError:
                    logger.warning('账号已被注销')
                    return
                except Exception as e:
                    logger.debug(e)

    def get_detail(self):
        while True:
            try:
                data = redis_conn.lpop(REDISKEY)

                encryptedId = data.split('-')[0]

                keyword = data.split('-')[1]

                url = 'https://job.dajie.com/recruit/apply/uncomplete/view?encryptedId={0}&pageType=search&conditionId=999999'.format(
                    encryptedId)
            except Exception as e:
                logger.warning('redis is empty')
                self.return_code += 1
                time.sleep(1200)
            else:
                external_id = data.split('-')[0]
                doc = collection.find_one({'external_id': 'dajie_' + str(external_id)})
                cos_id = str(ObjectId())

                if doc is None:

                    proxy = self.get_proxy()

                    proxies = {'http': proxy,
                               'https': proxy}


                    cookies = [
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M6EHUTzZS0ZEku4L9uEG8qZSmoxjmMbXFDBUV79H6zrVdYIFaj-caI68heXXUV_wFw**;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=MQRKv9_R865q9pICaVhi3BldN-8zjuwv2TJBYALZzyMAx49FuOj7r3ZA6Is2Pyg*;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=Mtr5uaquw7mzsYI5QnVuwK19GterIoAv77rK16jLEonuD7eNbOguF4Qipz4KYa2S;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=MtyOz1J9BBjByAU2vj01QeJ160fErXNtXIKZQMMdy5esPUtITouGUj5d23lGD5Om;'},

                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M7SgT3nTy8r5wuzk9m4Lqm__KPd18z1kVSle5MqmgZO2wBSFRdPfxjf0jbmbNYl6SA**;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M-tgNPwpBmgGNTRSX_Xnke3-DEnbpXLPnlzEXR-J_r3BSTaQFhqjmWggYKE7vIqYiQ**;'},

                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M_2Y2xN_H043hVRrPdr5jLBHhlB98-4ggnVd-M217KCNG1G8kfnSQgTLQSfTH9h2UA**;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=MoATMqLfZFhVDe5-hxhmdmBzqoHXHSYHKF8OOgxpVA4mmX79GtMt_TPAxHUIQf_2;'},
                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M8vjt1L_aKvxspGtWjXCJgQLp-bWkp01wQQopFFFE8IhDd934o8duHZaWYx6oCcU-g**;'},

                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=MU_ebcetq7k6GEZmCpWu1IOo-l0JOaURcUKFzrQPzNGCbvqUUIdWGmTRD6Ve6Y4*;'},

                        {
                            'Cookie': 'DJ_EU=http%3A%2F%2Fcampus.dajie.com%2F;dj_auth_v3=M4_t0MbrFCypaCwCnIi01FLmprdBmrrcWjnXd4ACUF6W6ZOnbDYtX0tXLVU5vxtGhA**;'
                                                                                                                                                           }


                    ]
                    cookie = random.choice(cookies)
                    try:
                        content = requests.get(url=url, headers=self.headers, cookies=cookie, proxies=proxies,
                                               timeout=10).content.decode('utf-8')

                        if '呃~操作过于频繁哦' in content:

                            logger.warning('呃~操作过于频繁哦')

                            self.request_failed += 1
                            logger.warning(self.request_failed)

                            redis_conn.rpush(REDISKEY, data)
                            time.sleep(30)
                        else:
                            if '简历更新时间' in content:
                                self.request_success += 1
                                doc = {
                                    'channel': 'system.dajie',
                                    '_created': datetime.utcnow(),
                                    'from_url': url,
                                    'external_id': 'dajie_' + str(external_id),
                                    'path': {'name': cos_id + '.html',
                                             'id': cos_id},
                                    '_search_options': {'keyword': keyword}
                                }
                                try:
                                    oid = collection.insert_one(doc, bypass_document_validation=False, session=None)
                                    logger.info('insert doc,external_id:{}'.format(external_id))
                                    # time.sleep(random.randint(2, 3))
                                    self.insert_docs += 1

                                    if self.insert_docs > 30:
                                        self.send_spider_logger()
                                        logger.info('已向spider_logger注入日志')
                                except Exception as e:
                                    logger.debug(e)
                                else:
                                    if oid:
                                        cos_client.put_object(
                                            Bucket='jobs',
                                            Body=content.encode('utf-8'),
                                            Key=cos_id,
                                            Metadata={
                                                'x-cos-meta-filename': quote('{}.{}'.format(cos_id, 'html'))},
                                            ContentType=MAPPING.get('html'))
                                        logger.info('cos注入一条数据')
                    except Exception as e:
                        logger.debug(e)
                        redis_conn.rpush(REDISKEY, data)
                else:
                    logger.warning('resume is repeated')
                    self.repeat_docs += 1



# 宠物美容,兽医,普工
keywords_1 = [
    '电器研发', 'CFO', '培训专员', '管道工程', '财务分析员', '舞蹈教练', '品牌管理', '自动化测试', '饮料研发', '项目总监', '美发', '电焊工',
    '网站编辑', 'C++', '财务助理', '供应商开发', '培训经理', '化学分析', 'CAD设计', '裁床', '文字编辑', 'HRIS', '保险管理', '助理猎头', 'Delphi',
    '薪酬经理', '品类管理', '服装制版', '面点师', '调酒师', '产品主管', '生物工程', '通信技术', '电器维修', '文档管理', '成本经理', '销售工程师', '牙科医生',
    '计量工程师', '融资专员', '锻造工程师', '铲车工', '器械推广', '机电工程师', '市场经理', '安防', '理货', '发行管理', '医药招商', 'SQLServer', '油漆工',
    '资料管理', '钣金工', '保姆护理', '事业部管理', '纺织品设计', '贸易助理', '珠宝鉴定', '采编', '业务助理', '导演', '社会工作者', '培训助教', '手机测试',
    '促销督导', '汽车美容', '供应链管理', '区域经理', '总机', '临时工', 'IC验证师', '数据分析师', '课程顾问', '电信网络', '资产评估', '音效师', '审计助理',
    '理财服务', '机械设计师', '促销员', '服装设计', '咨询管理师', 'IT文员', '资信分析', '商务经理', 'CTO', '游戏设计', '预结算', '工厂副厂长',
    'Oracle', '原画师', '船舶维修', '客户主管', '测试经理', '猎头助理', '企划专员', '保险代理', '器械销售', '矿产管理', '管培生', '录入', '合规顾问',
    '防损员', '高级建筑师', '移动开发', '人力总监', '拍卖师', '采购工程师', '电气设计', '营养师', '合同管理', '社会工作师', 'UE设计', '生产主管', '汽车总装师',
    '品牌助理', '项目管理', '风险稽查', '皮革工艺师', '机械保养', '电商经理', '仓库管理员', '教学管理', '家具设计', '银行主任', 'IT执行', '暖通工程', '护士',
    '信贷经理', '企划经理', '三维设计', '续期管理', 'CEO', '保安经理', '飞机维修', '展示设计', '制程工程师', '三维制作', '储备经理人', '按摩师', '资金管理',
    '成本会计', '电子保养', '飞机保养', '速递员', '音频工程师', '船舶保养', '生产经理', '物料管理员', '收藏品鉴定', '功能测试', '咨询主管', '仪表工程师', 'iOS',
    '融资经理', '内保', '摄像师', '朝鲜语翻译', '医药代表', '汽车机械师', '测试开发', '副校长', '销售讲师', '科研人员', '网店运营', '运输经理', '促销主管',
    'UX设计', 'ERP开发', '弱电', '实施顾问', '运营总监', '会务专员', '铸造工程师', '隧道工程', '电源开发', '建筑制图', 'UED设计', '放射科医师',
    '电商助理', '契约管理', '电脑操作', '总编辑', '空调总工', '公关经理', '有线传输', '规划与设计', '印刷操作', '媒介策划', '物业招商', '认证审核员', '养殖人员',
    '建筑测绘', '幕墙工程师', '物业顾问', '饮料检验', '公关主管', '港口工程', '服装设计师', '车床工', '纺织品销售', '专业顾问', '卖场管理', '生产工程师',
    '审计主管', ]



def main():
    crawl = Crawler()
    
    p = Pool(7)
    p.apply_async(crawl.proxy)
    for i in range(6):
        p.apply_async(crawl.get_detail)
    p.join()


if __name__ == "__main__":
    '''
    schedule.every().day.at("08:00").do(main)
    while True:
        schedule.run_pending()
        time.sleep(0.05)
    '''
    main()


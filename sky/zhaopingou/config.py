from redis import StrictRedis, ConnectionPool

connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True)
redis_conn = StrictRedis(connection_pool=connection_pool)
REDISKEY = 'zhaopingou_keyword'

keywords = ['web前端', '游戏开发/设计', 'Flash设计/开发', 'COCOS2D-X', 'html5', '脚本开发工程师', 'Python', '数据库工程师/管理员', 'JAVA工程师',]



cookies = [
    'Cookie:hrkeepToken=32278C3C914493BD73D494B7453454EE;',
    'Cookie:hrkeepToken=055D4B61103DE4FEFC3BA9659F0E4008;',
    'Cookie:hrkeepToken=20ED735400872426660636CFEA3C1791;',
    'Cookie:hrkeepToken=1874E2E0CBA6576A096E31A33D07FD25;',
    'Cookie:hrkeepToken=26A218DBD7C65A16BE7BBCC2A12A5769;',

]

proxypool = [{'http': '10.0.0.17:3128', 'https': '10.0.0.17:3128'},
             {'http': '10.0.40.16:3128', 'https': '10.0.40.16:3128'},
             {'http': '10.0.30.69:3128', 'https': '10.0.30.69:3128'},
             {'http': '10.0.40.13:3128', 'https': '10.0.40.13:3128'},
             {'http': '10.0.40.8:3128', 'https': '10.0.40.8:3128'}

             ]
data = ['32278C3C914493BD73D494B7453454EE-10.0.0.17:3128', '055D4B61103DE4FEFC3BA9659F0E4008-10.0.40.16:3128',
        '20ED735400872426660636CFEA3C1791-10.0.30.69:3128', '1874E2E0CBA6576A096E31A33D07FD25-10.0.40.13:3128',
        '26A218DBD7C65A16BE7BBCC2A12A5769-10.0.40.8:3128']

post_data1 = 'pageSize={0}&pageNo=25&keyStr={1}&companyName=&schoolName=&keyStrPostion=&postionStr=&startDegrees=-1&endDegress=-1&startAge=0&endAge=0&gender=-1&region=&timeType=-1&startWorkYear=-1&endWorkYear=-1&beginTime=&endTime=&isMember=-1&hopeAdressStr=&cityId=3&updateTime=&tradeId=&startDegreesName=&endDegreesName=&tradeNameStr=&regionName=&isC=0&is211_985_school=0&clientNo=&userToken={2}&clientType=2'

post_data2 = 'resumeHtmlId={0}&keyStr=&keyPositionName=&tradeId=&postionStr=&jobId=0&companyName=&schoolName=&clientNo=&userToken={1}&clientType=2'




def push_data():
    for keyword in keywords:
        redis_conn.rpush(REDISKEY, keyword)


def pop_data():
    redis_keywords = []
    for i in range(20):
        data = redis_conn.rpop(REDISKEY)
        redis_keywords.append(data)
    return redis_keywords


if __name__ == "__main__":
    print(keywords)

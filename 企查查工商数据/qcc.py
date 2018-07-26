
#coding=utf-8
from mongo_redis_mgr import MongoRedisUrlManager
from lxml import etree
import re
import time
import requests
import hashlib
import urllib.request
headers = {
    'Host':'www.qichacha.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    #'Cookie': 'UM_distinctid=16121a0f2a63c3-0713b5f898047-7c4f563c-100200-16121a0f2a74bf; zg_did=%7B%22did%22%3A%20%2216121a0f3648ff-0a562c97d5d608-7c4f563c-100200-16121a0f3653b4%22%7D; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201531107991117%2C%22updated%22%3A%201531108542503%2C%22info%22%3A%201530973560631%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22www.baidu.com%22%2C%22cuid%22%3A%20%22908f5b9623aa33e00d7cf0329d147118%22%7D; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1530974766,1530974783,1531098981,1531107992; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1531108522; CNZZDATA1254842228=2122350312-1516684102-https%253A%252F%252Fwww.baidu.com%252F%7C1531095313; hasShow=1; _uab_collina=153110579312886083820397; _umdata=C234BF9D3AFA6FE728D0E08CE6020BA352E98C93A914E1BEC5430A7DA6B66C1AA4648CBA48EFD1E6CD43AD3E795C914C3C92F3C286FAFD270C11F0E51E78AB77; PHPSESSID=ch0f3hn0s9dl2ffcmrlsu2uc87; acw_tc=AQAAACtekxw9KgAA6wA0pK1m2lRdpRg+'
}

PROXY_POOL_URL = 'http://localhost:5555/random'


def get_proxy():
    try:
        response = requests.get(PROXY_POOL_URL)
        if response.status_code == 200:
            return response.text
    except ConnectionError:
        return None
def get_content(url):
    print('开始取代理')
    ippo=get_proxy()
    print(ippo)
    proxy={'http':ippo}
    print(proxy)
    proxy_dict={"http":proxy}
    r = requests.get(url,headers=headers,proxies=proxy_dict)
    data=r.text
    return url,data
def extract_content(cur_url,data,depth):
    #抽取数据
    tree=etree.HTML(data)
    items=re.findall('/firm_[0-9a-zA-Z]*?.html',data) 
    company_name=''
    registered_capital=''
    contributed_capital=''
    management_forms=''
    established_data=''
    Organization_Number=''
    address=''
    business_scope=''
    try:
        
            company_name=tree.xpath('//div[@class="row title"]/h1')[0].text
            registered_capital=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"注册资本：")]/following-sibling::td[1]')[0].text
            contributed_capital=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"实缴资本：")]/following-sibling::td[1]')[0].text
            management_forms=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"经营状态：")]/following-sibling::td[1]')[0].text
            established_data=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"成立日期：")]/following-sibling::td[1]')[0].text
            Organization_Number=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"统一社会信用代码：")]/following-sibling::td[1]')[0].text
            address=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"企业地址：")]/following-sibling::td[1]')[0].text
            business_scope=tree.xpath('//table[@class="ntable"]//tr/td[contains(text(),"经营范围：")]/following-sibling::td[1]')[0].text
    except Exception as e:

         print('出错啦',e)

    print('++++++++++++')
    print(company_name)
    print(registered_capital)
    print(contributed_capital)
    print(management_forms)
    print(established_data)
    print(Organization_Number)
    print(address)
    print(business_scope)   
    print('-----------')

    if company_name !='':
        try:
            print('开始注入数据')
            db.db.qcdata.insert({
                    '_id': hashlib.md5(cur_url.encode('utf8')).hexdigest(), 
                    'url': cur_url, 
                    'company_name':company_name,
                    'registered_capital': registered_capital, 
                    'contributed_capital': contributed_capital, 
                    'management_forms': management_forms, 
                    'established_data':established_data,
                    'Organization_Number':Organization_Number,
                    'address':address,
                    'business_scope':business_scope
                     })
        except:
            pass    
    else:
        time.sleep(120) 
        
    
    db.finishUrl(cur_url)
    links = []
    items=re.findall('/firm_[0-9a-zA-Z]*?.html',data)    
    for i in items:
            
        fullurl='https://www.qichacha.com'+i
        db.enqueueUrl(fullurl,'new',depth+1)
        links.append(fullurl)
    print(links)
    db.set_url_links(cur_url,links)
if __name__=="__main__":
    db=MongoRedisUrlManager()
    #db.clear()
    #db.enqueueUrl('https://www.qichacha.com/firm_f56c21f7421c34ffac17e7fb653a02fc.html','new',0)
    while True:
        time.sleep(5)
        print('开始取数据')
        source=db.dequeueUrl()

        url,depth=source['url'],source['depth']

        try:
            url,data=get_content(url)
            print('---------2---------')
            extract_content(url,data,depth)
        except:
            pass
        



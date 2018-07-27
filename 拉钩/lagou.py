
#coding=utf-8
from mongo_redis_mgr import MongoRedisUrlManager
import urllib3
from lxml import etree
import re

request_headers = {
    'host': "www.lagou.com",
    'connection': "keep-alive",
    'cache-control': "no-cache",
    'upgrade-insecure-requests': "1",
    'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
    'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    'accept-language': "zh-CN,en-US;q=0.8,en;q=0.6"
}

dir_name='lagou/'
#dbmanager = MongoRedisUrlManager()
def get_page_content(cur_url):
    #global dir_name, dbmanager

    print( "downloading %s" % (cur_url))
    links = []
    try:
        http = urllib3.PoolManager()
        r = http.request('GET', cur_url, headers = request_headers)
  
        content=re.search('职位描述.*?</div>',r.data.decode('utf-8'),re.S).group().replace("<div>","").replace("</div>","").replace("</h3>","").replace("<br>","").replace("<p>","").replace("</p>","").replace("&nbsp;","").replace('\n','')
        print(content)
        salary=re.search('class="salary">(.*?)</span>',r.data.decode('utf-8'),re.S).group(1).replace("\n","").replace("<span>","").replace("</span>","")
        print(salary)
        company=re.search('alt=.*?公司',r.data.decode('utf-8'),re.S).group().replace('alt="','')
        print(company)
        people=re.search('[\d+-]?\d+人',r.data.decode('utf-8'),re.S).group()
        print(people)
        dbmanager.finishUrl(cur_url)
        items = re.findall('//www.lagou.com/jobs/\d+.html',r.data.decode('utf-8'))
        links = []
    
        for i in items:
        
             fullurl='https:'+i
             #print(fullurl)
             db.enqueueUrl(fullurl,'new')
             links.append(fullurl)
        print(links)

    except IOError as err:
        print( "get_page_content()", err )
        pass
    except Exception as err :
        print( "get_page_content()", err )
        pass
    try:
        items = re.findall('//www.lagou.com/jobs/\d+.html',r.data.decode('utf-8'))
        links = []
        for i in items:
             fullurl='https:'+i
             #print(fullurl)
             db.enqueueUrl(fullurl,'new')
             links.append(fullurl)
        print(links)
    except:
        pass



def crawl():
    while True:
        #print(cur_queue)
        #url = dequeuUrl()
        url=db.dequeueUrl()['url']
        get_page_content(url)

if __name__=="__main__":
    db=MongoRedisUrlManager()
    db.clear()
    db.enqueueUrl('https://www.lagou.com','new')
    crawl()
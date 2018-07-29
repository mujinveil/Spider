#coding=utf-8
import requests
from lxml import etree


def get_cities():
     headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
     'Accept-Encoding':'gzip, deflate',
     'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
     'Cache-Control': 'max-age=0',
     'Connection': 'keep-alive',
     'Cookie': 'KKOM_b949_saltkey=E9cDg2XR; KKOM_b949_lastvisit=1532757396; KKOM_b949_CITYPINYIN=shenzhen; KKOM_b949_440300_airpressure=1007; KKOM_b949_lastact=1532761057%09portal.php%09diaochang; Hm_lvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075; Hm_lpvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075',
     'Host': 'www.haodiaoyu.com',
     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

     response=requests.get(url='http://www.haodiaoyu.com/diaochang/',headers=headers)
     content=response.text

     tree=etree.HTML(content)
     nodes=tree.xpath('//div[@class="city-names"]/a/@href')
     city_names=[]
     for i in nodes:
     	city_name=i.replace('./diaochang/','')
     	city_name=city_name.replace('/','')
     	city_names.append(city_name)
     return city_names

if __name__ == '__main__':

     city_names=get_cities()
     print(city_names)





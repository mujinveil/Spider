#coding=utf-8
import requests
from lxml import etree
from  pypinyin import lazy_pinyin


def get_cities():
     headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding':'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'hotel.meituan.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

     response=requests.get(url='http://www.meituan.com/changecity/',headers=headers)
     content=response.text

     tree=etree.HTML(content)
     cities=tree.xpath("//div[@class='city-area']/span[@class='cities']/a/text()")
     pycities=[]
     for city in cities:
           city="".join(lazy_pinyin(city))
           pycities.append(city)

     return pycities

if __name__ == '__main__':

     city_names=get_cities()





#coding=utf-8
import requests
from lxml import etree
import pymysql 
import pymongo
from  city_name import get_cities
from selenium import webdriver
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.wait  import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

client=pymongo.MongoClient('localhost')
db=client['meituan']
collections=db['meituan_technique']
collections.create_index([('link',pymongo.ASCENDING)])
driver=webdriver.Chrome()



class crawl_hotel(object):
      def __init__(self):
          self.headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Host': 'hotel.meituan.com',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

      
      def get_response(self,url):
         response=requests.get(url=url,headers=self.headers).text
         #print(response)
         content=etree.HTML(response)

         return content


      def total_num(self,base_url):

          tree=self.get_response(url=base_url)
          total_page=int(tree.xpath('//li[@class="next"]/preceding-sibling::li[1]/a/text()|//li[@class=" next"]/preceding-sibling::li[1]/a/text()')[0])
          print(total_page)
          return total_page



      def get_content(self,base_url,page):
           
           url=base_url+'/pn={0}'.format(page)
           #driver.add_cookie(self.cookie)
           driver.get(url)
           wait=WebDriverWait(driver,10)
           wait.until(EC.presence_of_element_located((By.XPATH,'//article[@class="poi-item"]')))
           wait.until(EC.presence_of_element_located((By.XPATH,'//div[@class="service-icons"]')))
           nodes=driver.find_elements_by_xpath('//article[@class="poi-item"]')
           print('正在抓取',url)
           item={}
           for node in nodes:
                item['link']=node.find_element_by_xpath('.//a[@class="poi-title"]').get_attribute('href')
                item['hotel_name']=node.find_element_by_xpath('.//a[@class="poi-title"]').text
                item['address']=node.find_element_by_xpath('.//div[@class="poi-address"]').text
                item['service']=node.find_element_by_xpath('.//div[@class="service-icons"]').text
                item['rate']=node.find_element_by_xpath('.//div[@class="poi-grade"]').text
                item['consume-num']=node.find_element_by_xpath('.//div[@class="poi-buy-num"]').text
                item['price']=node.find_element_by_xpath('.//div[@class="poi-price"]/em').text
                print(item)
                print('开始注入数据')
                collections.insert(dict(item))
                #collections.update({'link':item.get('link')},{'$set':item},True)

           

 
def main(base_url):              
    crawl=crawl_hotel()

    total_page=crawl.total_num(base_url)

    for i in range(total_page):
         i=i+1
         item=crawl.get_content(base_url,i)




if __name__ == '__main__':
   #bases_url=['http://www.haodiaoyu.com/jiqiao/tiaopiao/','http://www.haodiaoyu.com/jiqiao/siji/','http://www.haodiaoyu.com/jiqiao/diaofa/']
   city_names=get_cities()
   for city_name in city_names:
      base_url='http://hotel.meituan.com/{0}'.format(city_name)
      main(base_url)




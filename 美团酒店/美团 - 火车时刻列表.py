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

#chrome_options=webdriver.ChromeOptions()
#chrome_options.add_argument('--headless')


client=pymongo.MongoClient('localhost')
db=client['train']
collections=db['train_info']
#driver=webdriver.Chrome(chrome_options=chrome_options)
driver=webdriver.Chrome()


class crawl_train(object):
      def __init__(self):
          self.headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Host': 'hotel.meituan.com',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

      def total_num(self,driver,base_url):
          
          driver.get(base_url)
          wait=WebDriverWait(driver,10)
          wait.until(EC.element_to_be_clickable((By.XPATH,'//a[@data-index="next"]/../preceding-sibling::li[1]')))
          total_page=int(driver.find_element_by_xpath('//a[@data-index="next"]/../preceding-sibling::li[1]').text)
          print(total_page)
          return total_page


      def go_next(self,driver):
          driver.find_element_by_xpath('//a[@data-index="next"]').click()


      def get_content(self,driver):
           

           
           wait=WebDriverWait(driver,10)
           wait.until(EC.element_to_be_clickable((By.XPATH,'//a[@data-index="next"]')))
           nodes=driver.find_elements_by_xpath('//ul[@class="timetable-body"]/ul')
           ##print('正在抓取',url)
           item={}
           for node in nodes:
                item['url']=driver.current_url
                item['destination']=driver.find_element_by_xpath('//h4[@class="timetable-title"]').text.replace('火车时刻表','')
                item['班次']=node.find_element_by_xpath('.//li[1]/span[1]/span[1]').text
                item['type']=node.find_element_by_xpath('.//li[2]').text
                item['end_time']=node.find_element_by_xpath('.//li[3]').text
                item['start_time']=node.find_element_by_xpath('.//li[4]').text
                item['rest_time']=node.find_element_by_xpath('.//li[5]').text
                item['start_station']=node.find_element_by_xpath('.//li[6]').text
                item['end_station']=node.find_element_by_xpath('.//li[7]').text
                print(item)
                collections.insert(dict(item))
                #collections.update({'link':item.get('link')},{'$set':item},True)

           

 
def main(base_url):              
    crawl=crawl_train()

    total_page=crawl.total_num(driver,base_url)
    #因为已经执行过一次，这里翻页次数要减1
    total_page=total_page-1

    for i in range(total_page):

       crawl.get_content(driver)
       crawl.go_next(driver)





if __name__ == '__main__':
   #bases_url=['http://www.haodiaoyu.com/jiqiao/tiaopiao/','http://www.haodiaoyu.com/jiqiao/siji/','http://www.haodiaoyu.com/jiqiao/diaofa/']
   city_names=['http://www.meituan.com/train/timetableshz/',
                'http://www.meituan.com/train/timetablegz/',
                'http://www.meituan.com/train/timetablesh/',
                'http://www.meituan.com/train/timetablebj/',
                'http://www.meituan.com/train/timetablehz/',
                'http://www.meituan.com/train/timetablecd/',
                'http://www.meituan.com/train/timetablenj/',
                'http://www.meituan.com/train/timetablezz/',
                'http://www.meituan.com/train/timetablexm/',
                'http://www.meituan.com/train/timetablecq/',
                'http://www.meituan.com/train/timetablesz/',
                'http://www.meituan.com/train/timetabledl/',
                'http://www.meituan.com/train/timetablexa/',
                'http://www.meituan.com/train/timetablefz/',
                'http://www.meituan.com/train/timetablejn/',
                'http://www.meituan.com/train/timetableqd/',
                'http://www.meituan.com/train/timetablehf/',
                'http://www.meituan.com/train/timetablewh/',
                'http://www.meituan.com/train/timetablenb/',
                'http://www.meituan.com/train/timetableheb/']

   for city_name in city_names:

      main(city_name)




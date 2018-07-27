#coding=utf-8
from selenium import webdriver
from lxml import etree
import time

import threading
import sys
import io
from selenium.webdriver.support.ui import WebDriverWait
import json
import pymongo

client=pymongo.MongoClient(host='localhost',port=27017)
db=client.yw
collection=db['yw']
driver=webdriver.Chrome()
driver.set_window_size(1300,900)
wait=WebDriverWait(driver,30)

def log_in(driver):
    driver.get('https://data.pharmacodia.com/web/homePage/index')
    driver.find_element_by_xpath('//a[@class="undline_n login_color login_color_bg"]').click()
    driver.find_element_by_id('user_idname').send_keys('15013586458')
    driver.find_element_by_id('user_idpsw').send_keys('') 
    driver.find_element_by_id('user_login_btn').click()
    time.sleep(2)
    driver.find_element_by_id('login_get_code').click()
    a=input('请输入图片验证码：')
    b=input('请输入手机验证码：')   
    driver.find_element_by_id('sjtxyz').send_keys(a)
    driver.find_element_by_id('sjdxyz').send_keys(b)
    driver.find_element_by_id('user_login_img_btn').click()                          
    #driver.find_element_by_xpath("//p[contains(text(),'中国注册')]").click()
    driver.get('https://data.pharmacodia.com/web/cde/query?ns=0&jq=&ys=1')

def get_content(driver):
   nodes=driver.find_elements_by_xpath('//ul[@class="market_content_bottom_title market_content_bottom_content"]')
   item={}

   for node in nodes:
       item['date']=node.find_element_by_xpath('.//li[1]/span').text
       item['product_num']=node.find_element_by_xpath('.//li[2]/span').text
       item['product_name']=node.find_element_by_xpath('.//li[3]/span').text
       item['product_type']=node.find_element_by_xpath('.//li[4]/span').text
       item['bid_type']=node.find_element_by_xpath('.//li[6]/span').text
       item['company']=node.find_element_by_xpath('.//li[8]/span').text
       item['status']=node.find_element_by_xpath('.//li[9]/span').text
       item['status_begin_date']=node.find_element_by_xpath('.//li[13]/span').text
       write_to_file(item)
       collection.insert(dict(item))

def write_to_file(content):
    with open('yw.txt','a',encoding='utf-8') as f:
      f.write(json.dumps(content,ensure_ascii=False)+'\n')

def go_next(driver):
    next_page=driver.find_element_by_xpath('//span[@class="col_1"]/a[contains(text(),"下一页")]')
    next_page.click()




if __name__ == '__main__':
   log_in(driver)
   
   while True:
      try:
          get_content(driver)
          go_next(driver)
          time.sleep(1)
      except Exception as e:
         print(e)
         break
   driver.close()
   driver.quit()
   





#coding=utf-8
from selenium import webdriver
from lxml import etree
import time

import threading
import sys
import io
from selenium.webdriver.support.ui import WebDriverWait
import pymongo
client=pymongo.MongoClient('localhost',27017)
db=client['db']


driver=webdriver.Chrome()
driver.set_window_size(1300,900)
wait=WebDriverWait(driver,30)


def log_in(username,password):
    try:
       driver.get('https://accounts.douban.com/login?')
       driver.find_element_by_id('email').send_keys(username)
       driver.find_element_by_id('password').send_keys(password)
       driver.find_element_by_xpath('//input[@value="登录"]').click()
    except Exception as e:
       print(e)
       pass

def my_order(driver,page):
    page=str(page*20)
    url='https://movie.douban.com/subject/1292064/comments?start={}&limit=20&sort=new_score&status=P'.format(page)
    driver.get(url)
    nodes=driver.find_elements_by_xpath('//div[@class="comment-item"]/div[@class="comment"]')
    for node in nodes:
        item={}
        item['name']=node.find_element_by_xpath('.//span[2]/a').text
        item['date']=node.find_element_by_xpath('.//span[@class="comment-time"]|.//span[@class="comment-time "]').get_attribute('title')
        item['zan']=node.find_element_by_xpath(".//h3//span[@class='votes']").text
        item['comment']=node.find_element_by_xpath('.//p/span').text
        db['truemanshow'].insert(dict(item))


if __name__ == '__main__':

    username=''
    password=''
 
    log_in(username,password)
    try:
      for i in range(25):
        my_order(driver,i)
    except Exception as e:
       print(e)
       pass
    time.sleep(5)
    driver.close()
    driver.quit()






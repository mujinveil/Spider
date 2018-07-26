#coding=utf-8
from selenium import webdriver
from lxml import etree
import time

import threading
import sys
import io
from selenium.webdriver.support.ui import WebDriverWait
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')

driver=webdriver.Chrome()
driver.set_window_size(1300,900)
wait=WebDriverWait(driver,30)




def log_in(username,password):
    try:
       driver.get('https://passport.jd.com/uc/login?ltype=logout')
       driver.find_element_by_link_text('账户登录').click()

       driver.find_element_by_id('loginname').send_keys(username)
       driver.find_element_by_id('nloginpwd').send_keys(password)
       driver.find_element_by_id('loginsubmit').click()
       
       
       try: 
           driver.find_element_by_id('authcode')   
           a=str(input('请输入验证码：'))
           driver.find_element_by_id('authcode').send_keys(a)
           driver.find_element_by_id('loginsubmit').click()
       except Exception as e:
           print(e)
           pass
       #time.sleep(10)
       
    except Exception as e:
       print(e)
       pass
def scroll_to_window2(driver):
    now_handle = driver.current_window_handle
    driver.find_element_by_link_text('我的订单').click()
    all_handles = driver.window_handles 
    
    for handle in all_handles:  

        if handle!=now_handle:     

            #输出待选择的窗口句柄  

            print (handle)  

            driver.switch_to_window(handle)  

            time.sleep(1)
    return driver 

def my_order(driver):
    nodes=driver.find_elements_by_xpath('//table[@class="td-void order-tb"]/tbody')
    for node in nodes:
        dealtime=node.find_element_by_xpath(".//tr[2]//span[contains(@class,'dealtime')]").text
        ordernumber=node.find_element_by_xpath('.//tr[2]//span[3]/a').text
        order_shop=node.find_element_by_xpath('.//div/span[@class="order-shop"]').text
        product_name=node.find_element_by_xpath('./tr[3]//div//div/a[@class="a-link"]').get_attribute('title')
        product_num=node.find_element_by_xpath('.//tr[3]//div[@class="goods-number"]').text
        amount=node.find_element_by_xpath('.//div[@class="amount"]/span[1]').text
        config=node.find_element_by_xpath('.//div[@class="amount"]/span[2]').text
        statu=node.find_element_by_xpath('//div[@class="status"]/span').text


if __name__ == '__main__':
    #username=input('请输入账号：')
    #password=input('请输入密码：')
    driver.get('https://passport.jd.com/uc/login?ltype=logout')
    username=''
    password=''
    time.sleep(5)
    log_in(username,password)
    #driver.get("http://trade.jr.jd.com/centre/browse.action")
    scroll_to_window2(driver)
    my_order(driver)
    time.sleep(5)
    driver.close()
    driver.quit()






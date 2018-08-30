#coding=utf-8
from selenium import webdriver

from selenium.webdriver.support.ui import WebDriverWait
import time


driver=webdriver.Chrome()
driver.set_window_size(1300,900)


def log_in():
    driver.get('https://graph.qq.com/oauth2.0/show?which=Login&display=pc&response_type=code&client_id=101007794&redirect_uri=http%3A%2F%2Fwww.dajie.com%2Faccount%2Fqq%2Fcallback%3Fd.cb%3D0&state=dc62e3983d19ca9e806688c96b12abbaa8a24506%27')
    driver.switch_to.frame('ptlogin_iframe')
    driver.find_element_by_id('switcher_plogin').click()
    driver.find_element_by_id('u').send_keys('1518219336')
    driver.find_element_by_id('p').send_keys('')
    driver.find_element_by_id('login_button').click()
    driver.find_element_by_xpath('//a[@class="recruit"]').click()


def search(keyword):
    driver.find_element_by_xpath('//input[@name="keyword"]').send_keys(keyword)
    driver.find_element_by_xpath('//i[@class="icon-search"]').click()
    nodes=driver.find_elements_by_xpath('//div[@class="center-box"]')
    item=[]
    for node in nodes:

        link=node.find_element_by_xpath('.//a[@class="readPropile"]').get_attribute('href')
        item.append(link)
    print(item)

def get_detail(url):
    driver.get(url)
    conent=driver.page_source
   

if __name__=="__main__":
   
   try:
     log_in()
     keyword='java'
     search(keyword)
     time.sleep(5)
   except Exception as e:
     print(e)
   
   driver.close()
   driver.quit()

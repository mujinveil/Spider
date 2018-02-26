
# coding: utf-8

# In[40]:

from selenium import webdriver
browser=webdriver.Chrome()


# In[41]:

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# In[52]:

#from lxml import etree
from pyquery import PyQuery as pq
wait=WebDriverWait(browser,10)
def search():
    try:
        browser.get('https://www.taobao.com')
        input=wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'#q')))
        submit=wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,'#J_TSearchForm > div.search-button > button')))
        input.send_keys('美食')
        submit.click()
        get_products()
        total=wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'#mainsrp-pager > div > div > div > div.total')))
        return total.text
    except TimeoutException :
        return search()

def get_products():
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'#mainsrp-itemlist .items .item')))
    html=browser.page_source
    doc=pq(html)
    items=doc('#mainsrp-itemlist .items .item').items()
    for item in items:
        product={
            'image':item.find('.pic .img').attr('src'),
            'price':item.find('.price').text(),
            'deal':item.find('.deal-cnt').text()[:-3],
            'title':item.find('.title').text(),
            'shop':item.find('.shop').text(),
            'location':item.find('.loaction').text()
            }
        print(product)
    
def next_page(page_number):
    try:
        input=wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'#mainsrp-pager > div > div > div > div.form > input')))
        submit=wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,'#mainsrp-pager > div > div > div > div.form > span.btn.J_Submit')))
        input.clear()
        input.send_keys(page_number)
        submit.click()
        wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR,'#mainsrp-pager > div > div > div > ul > li.item.active > span'),str(page_number)))
        get_products()
    except TimeoutException :
        return next_page(page_number)


# In[53]:

import re
if __name__=="__main__":
    total=search()
    total=int(re.search('共(.*?)页',total,re.S).group(1))
    print(total)
    
    for i in range(2,total+1):
        next_page(i)


# In[ ]:




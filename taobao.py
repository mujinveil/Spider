# coding=utf-8
import time
from pyquery import PyQuery as pq
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pynput.mouse import Button, Controller as c1
from urllib.parse import quote

# import os
# import time
# import pyautogui as pag
#
# while True:
#     print('ctrl -c')
#
#     x, y = pag.position()
#
#     position = "Position:" + str(x).rjust(4) + ',' + str(y).rjust(4)
#
#     print(position)
#
#     time.sleep(1)
#
#     os.system('cls')
#
# print("ending")

def login():
    url = 'https://login.taobao.com/'
    driver.get(url)
    login_before = wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(text(),'密码登录')]")))
    login_before.click()
    send_name = wait.until(EC.presence_of_element_located((By.ID, 'TPL_username_1')))
    send_name.send_keys('')
    send_pd = wait.until(EC.presence_of_element_located((By.ID, 'TPL_password_1')))
    send_pd.send_keys('')
    login = wait.until(EC.presence_of_element_located((By.ID, 'J_SubmitStatic')))
    login.click()
    time.sleep(3)
    mouse.position = (910, 502)
    mouse.press(Button.left)
    mouse.move(11806, 500)
    mouse.release(Button.left)

    send_passwd = wait.until(EC.presence_of_element_located((By.ID, 'TPL_password_1')))
    send_passwd.clear()

    send_passwd.send_keys('')
    login = wait.until(EC.presence_of_element_located((By.ID, 'J_SubmitStatic')))
    login.click()

    time.sleep(5)


def search(keyword):
    url = 'https://s.taobao.com/search?q={0}'.format(quote(keyword))
    driver.get(url)


def index_page(page):
    print('正在爬取第', page, '页')
    try:
        if page >= 1:
            input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#mainsrp-pager div.form > input')))

            submit = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '#mainsrp-pager div.form > span.btn.J_Submit')))
            input.clear()
            input.send_keys(page)
            submit.click()
            wait.until(
                EC.text_to_be_present_in_element((By.CSS_SELECTOR, '#mainsrp-pager li.item.active > span'), str(page)))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.m-itemlist .items .item')))

            get_products()
    except Exception as e:
        if '抱歉！页面无法访问' in driver.page_source:
            driver.back()
        else:
            print(e)
            time.sleep(10)
            mouse.position = (545, 435)

            mouse.press(Button.left)

            mouse.move(822, 432)

            mouse.release(Button.left)

            if '哎呀，出错了' in driver.page_source:
                # mouse.position = (674, 436)
                refresh = wait.until(EC.element_to_be_clickable((By.XPATH, '//span[@class="nc-lang-cnt"]/a[contains(text(),"刷新")]')))
                refresh.click()
                # mouse.press(Button.left)

        index_page(page)


def get_products():
    html = driver.page_source

    doc = pq(html)

    items = doc('#mainsrp-itemlist .items .item').items()

    for item in items:
        product = {

            'image': item.find('.pic .img').attr('data-src'),

            'price': item.find('.price').text(),

            'deal': item.find('.deal-cnt').text(),

            'title': item.find('.title').text(),
            'shop': item.find('.shop').text(),
            'location': item.find('.location').text()}

        print(product)


if __name__ == "__main__":
    mouse = c1()

    options = webdriver.ChromeOptions()

    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    options.add_experimental_option('excludeSwitches', ['enable-automation'])

    ua = 'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'

    options.add_argument(ua)

    driver = webdriver.Chrome('F:\BaiduNetdiskDownload\chromedriver.exe', options=options)

    driver.maximize_window()

    wait = WebDriverWait(driver, 10)
    login()

    search('手机')

    for i in range(100):
        index_page(i + 1)

    driver.close()

    driver.quit()

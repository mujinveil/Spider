# coding=utf-8
import re

import time

from io import BytesIO

from utils import SHlogger

from PIL import Image

import requests

from redis import StrictRedis, ConnectionPool

from selenium import webdriver

from selenium.webdriver import ActionChains

from selenium.webdriver.common.by import By

from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.ui import WebDriverWait

from hashlib import md5









connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True)

redis_conn = StrictRedis(connection_pool=connection_pool)

REDISKEY = 'dajie'



EMAIL = 'huewinshd@126.com'

PASSWORD = '2gtHu38D'

logger=SHlogger().logger



CHAOJIYING_USERNAME = 'wlccgp3'

CHAOJIYING_PASSWORD = 'wlcc1991'

CHAOJIYING_SOFT_ID = 897109

CHAOJIYING_KIND = 9004



chromeOptions = webdriver.ChromeOptions()

IP = '10.0.40.16:3128'

chromeOptions.add_argument("--proxy-server=http://{0}".format(IP))



class Chaojiying(object):



    def __init__(self, username, password, soft_id):

        self.username = username

        self.password = md5(password.encode('utf-8')).hexdigest()

        self.soft_id = soft_id

        self.base_params = {

            'user': self.username,

            'pass2': self.password,

            'softid': self.soft_id,

        }

        self.headers = {

            'Connection': 'Keep-Alive',

            'User-Agent': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)',

        }



    def post_pic(self, im, codetype):

        """

        im: 图片字节

        codetype: 题目类型 参考 http://www.chaojiying.com/price.html

        """

        params = {

            'codetype': codetype,

        }

        params.update(self.base_params)

        files = {'userfile': ('ccc.jpg', im)}

        r = requests.post('http://upload.chaojiying.net/Upload/Processing.php', data=params, files=files,

                          headers=self.headers)

        return r.json()



    def report_error(self, im_id):

        """

        im_id:报错题目的图片ID

        """

        params = {

            'id': im_id,

        }

        params.update(self.base_params)

        r = requests.post('http://upload.chaojiying.net/Upload/ReportError.php', data=params, headers=self.headers)

        return r.json()





class DajieCrawler():

    def __init__(self):

        self.url = 'https://www.dajie.com/account/login'

        options = webdriver.ChromeOptions()

        options.add_argument('--headless')

        self.browser = webdriver.Chrome()#(options=options)

        #self.browser.set_window_size(1000,1300)

        self.wait = WebDriverWait(self.browser, 20)

        self.email = EMAIL

        self.password = PASSWORD

        self.chaojiying = Chaojiying(CHAOJIYING_USERNAME, CHAOJIYING_PASSWORD, CHAOJIYING_SOFT_ID)



    def __del__(self):

        self.browser.close()



    def open(self):

        """

        打开网页输入用户名密码

        :return: None

        """

        self.browser.delete_all_cookies()

        self.browser.get(self.url)

        email = self.wait.until(EC.presence_of_element_located((By.ID, 'loginName')))

        password = self.wait.until(EC.presence_of_element_located((By.XPATH, '//input[@name="password"]')))

        email.send_keys(self.email)

        password.send_keys(self.password)



    def get_touclick_button(self):

        """



        点击出现验证码的按钮

        :return:

        """

        button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'capbutton_btn')))

        return button



    def get_touclick_element(self, id_name):

        """

        获取验证图片对象

        :return: 图片对象

        """

        element = self.wait.until(EC.presence_of_element_located((By.ID, id_name)))

        return element



    def get_screenshot(self):

        """

        获取网页截图

        :return: 截图对象

        """

        screenshot = self.browser.get_screenshot_as_png()

        screenshot = Image.open(BytesIO(screenshot))

        return screenshot



    def get_touclick_image(self, id_name, name='captcha.png'):

        """

        获取验证码图片

        :return: 图片对象

        """

        element = self.wait.until(EC.presence_of_element_located((By.ID, id_name)))

        time.sleep(2)

        location = element.location

        size = element.size

        top, bottom, left, right = location['y'], location['y'] + size['height'], location['x'], location['x'] + size[

            'width']





        screenshot = self.get_screenshot()

        captcha = screenshot.crop((left, top, right, bottom))

        captcha.save(name)

        return captcha



    def get_points(self, captcha_result):

        """

        解析识别结果

        :param captcha_result: 识别结果

        :return: 转化后的结果

        """

        groups = captcha_result.get('pic_str').split('|')

        locations = [[int(number) for number in group.split(',')] for group in groups]

        print(locations)

        return locations



    def touch_click_words(self, locations, id_name):

        """

        点击验证图片

        :param locations: 点击位置

        :return: None

        """

        for location in locations:

            print(location)

            ActionChains(self.browser).move_to_element_with_offset(self.get_touclick_element(id_name), location[0],

                                                                   location[1]).click().perform()

            time.sleep(1)



    def touch_click_verify(self):

        """

        点击验证按钮

        :return: None

        """



        button = self.wait.until(EC.element_to_be_clickable((By.ID,'verifyId')))

        button.click()

        time.sleep(2)



    def search(self, keyword):

        self.browser.get('https://job.dajie.com/search/talent/condition/list/index?conditionId=&schoolType='

                         '&special=&schoolIds=&homeCity=&overseas=&majorIds=&language=&graduateYears=&search'

                         'Range=&keyWordMode=0&keyword=&isIncludeKeyword=&corpName=&isCompanyNameIncludeKey'

                         'word=&positionFunction=110109&industry=&city=&workExperience=&minDegree=&maxDegree'

                         '=&minAge=&maxAge=&gender=&salary=&updateType=&filterNoImage=0&filterReaded=0&filte'

                         'rDownResume=0&sortMode=2&capToken=&_CSRFToken=ZTZTMeR0ouwu8GlvyaTwc3mgcMuektVrECwOMfg*&scrollTop=0')

        for i in range(100):

            i = i + 1

            try:

                if '呃~操作过于频繁哦~' in self.browser.page_source:

                    image = self.get_touclick_image(id_name='code_v1', name='detail_captcha.png')

                    bytes_array = BytesIO()

                    image.save(bytes_array, format='PNG')

                    result = self.chaojiying.post_pic(bytes_array.getvalue(), CHAOJIYING_KIND)

                    locations = self.get_points(result)

                    self.touch_click_words(locations, 'code_v1')

                    self.wait.until(EC.element_to_be_clickable((By.ID, 'verifyId_v1'))).click()

                    time.sleep(2)

            except Exception as e:

                logger.debug(e)

                pass

            try:



                nodes = self.wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'center-box')))

                for node in nodes:

                    link = node.find_element_by_xpath('.//a[@class="readPropile"]').get_attribute('href')



                    uid = re.search('conditionId=(\d+)', link).group(1)

                    encryptedId = re.search('encryptedIds=(.*?)&condition', link).group(1)

                    external_id = encryptedId + '-' + str(uid) + '-' + keyword

                    redis_conn.rpush(REDISKEY, external_id)

                    logger.info('{0} is inserted into redis'.format(external_id))

                input = self.wait.until(EC.presence_of_element_located((By.XPATH, '//input[@class="page_jump"]')))

                input.clear()

                input.send_keys(i)

                button = self.wait.until(EC.element_to_be_clickable((By.XPATH, '//span[@class="go_jump"]')))

                button.click()

                time.sleep(3)

            except Exception as e:

                logger.debug(e)

                time.sleep(3)

                pass







    def crack(self):

        """

        破解入口

        :return: None

        """

        self.open()

        # 点击验证按钮

        button = self.get_touclick_button()

        button.click()

        # 获取验证码图片

        image = self.get_touclick_image('wrap_captcha')

        bytes_array = BytesIO()

        image.save(bytes_array, format='PNG')

        # 识别验证码

        result = self.chaojiying.post_pic(bytes_array.getvalue(), CHAOJIYING_KIND)



        locations = self.get_points(result)

        self.touch_click_words(locations, 'wrap_captcha')

        self.touch_click_verify()



        self.search('电话销售')





if __name__ == '__main__':

    crack = DajieCrawler()

    crack.crack()

# coding=utf-8



import time

from io import BytesIO



from PIL import Image

from selenium import webdriver

from selenium.common.exceptions import TimeoutException

from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.common.by import By

from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.ui import WebDriverWait

from redis import StrictRedis, ConnectionPool





connection_pool = ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True)

redis_conn = StrictRedis(connection_pool=connection_pool)





class zpg_login(object):

    def __init__(self, usernumber, password, browser):

        self.url = 'http://qiye.zhaopingou.com/signin'

        self.browser = browser

        self.usernumber = usernumber

        self.password = password

        self.wait = WebDriverWait(self.browser, 20)



    '''

    def __del__(self):

        self.browser.close()

    '''



    def open(self):

        """

        打开网页输入用户名密码

        :return:

        """

        self.browser.delete_all_cookies()

        self.browser.get(self.url)

        username = self.wait.until(EC.presence_of_element_located((By.ID, 'form_login_name')))

        password = self.wait.until(EC.presence_of_element_located((By.ID, 'form_login_pwd')))

        username.send_keys(self.usernumber)

        password.send_keys(self.password)



    def password_error(self):

        """

        判断是否密码错误

        :return:

        """

        try:

            return WebDriverWait(self.browser, 5).until(

                EC.text_to_be_present_in_element((By.XPATH, "//div[@class='from-error']/p"), '您输入的密码不正确'))

        except TimeoutException:

            return False



    def get_image(self):

        """

        获取验证码位置

        :return: 验证码位置元组

        """

        self.browser.switch_to.frame(self.wait.until(EC.presence_of_element_located((By.XPATH,"//iframe[contains(@id,'captcha_widget')]"))))

        #self.browser.switch_to.frame(self.browser.find_element_by_xpath("//iframe[contains(@id,'captcha_widget')]"))

        submit = self.wait.until(EC.element_to_be_clickable((By.XPATH, '//span[@class="captcha-widget-text"]')))



        submit.click()



        time.sleep(5)

        self.browser.switch_to.parent_frame()

        try:

            img_src = self.browser.find_element_by_xpath("//iframe[contains(@id,'captcha_frame')]").get_attribute('src')

            js = 'window.open("{0}");'.format(img_src)

            self.browser.execute_script(js)

            all_handles = self.browser.window_handles

            self.browser.switch_to.window(all_handles[1])

            img = self.wait.until(EC.presence_of_element_located((By.XPATH, '//div[@class="captcha-list"]')))



        except TimeoutException:

            print('未出现验证码')



        location = img.location



        size = img.size



        top, bottom, left, right = location['y'], location['y'] + size['height'], location['x'], location['x'] + size[

            'width']

        screenshot = self.browser.get_screenshot_as_png()

        screenshot = Image.open(BytesIO(screenshot))

        captcha = screenshot.crop((left, top, right, bottom))

        captcha.save('ab.png')



        self.browser.switch_to.window(all_handles[0])

        self.browser.switch_to.frame(self.browser.find_element_by_xpath("//iframe[contains(@id,'captcha_frame')]"))



        return captcha



    def is_white(self, image, x, y):

        """

        判断是否为白色点

        :param image:

        :param x:

        :param y:

        :return:

        """

        pixel = image.load()[x, y]

        threshold = 5

        if abs(pixel[0] - 255) < threshold and abs(pixel[1] - 255) < threshold and abs(pixel[2] - 255) < threshold:

            return True

        else:

            return False



    def get_position(self, image):

        """

        找到白色点的位置

        :param image:

        :return:

        """

        for x in range(image.width):

            for y in range(image.height):

                if self.is_white(image, x, y) and self.is_white(image, x + 5, y + 5):

                    return x, y



    def click_target(self, x, y):

        target = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='captcha-list']")))

        ActionChains(self.browser).move_to_element_with_offset(target, x, y).click().perform()









    def run(self):

        while True:

            try:

                data = redis_conn.lpop('zhaopingou_parse')

                external_id = data.split('_')[1]

                print(external_id)

            except:

                print('redis is empty')

                break

            else:

                self.browser.get('http://qiye.zhaopingou.com/resume/detail?resumeId={0}'.format(external_id))

                time.sleep(2)

                content = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME ,'resume-information')))

                #content=self.browser.find_element_by_xpath('//div[@class="resume_details_outer mianfei_service_orange"]').text

                print(content.text)





    def main(self):

        """

        破解验证码位置

        :return:

        """

        self.open()

        time.sleep(5)

        image = self.get_image()

        x, y = self.get_position(image)

        self.click_target(x, y)

        self.browser.switch_to.parent_frame()

        submit = self.wait.until(EC.element_to_be_clickable((By.ID, 'form_login')))

        time.sleep(5)

        submit.click()

        time.sleep(1)

        self.run()




if __name__ == "__main__":

    browser = webdriver.Chrome()

    zpg_login('18603054348', 'GU9SAePl', browser).main()
#coding=utf-8
import requests
from lxml import etree

class log_in(object):
     def __init__(self):
          self.headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Host': 'www.lagou.com',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}


          self.post_url='https://passport.lagou.com/login/login.json'
          self.logined_url='https://www.lagou.com/jobs/positionAjax.json?city=%E5%8C%97%E4%BA%AC&needAddtionalResult=false'
          self.session=requests.Session()


     def login(self,phone):
          post_data={'isValidate':'true',
                     'username':phone,
                     'password':'b15bab372d2d682280b09f4605a40468',
                     'request_form_verifyCode':'',
                     'submit':''}
          response=self.session.post(self.post_url,data=post_data,headers=self.headers)

          data={'pn':'2','kd':'python','first':'false'}
          response=self.session.post(url=self.logined_url,data=data,headers=self.headers)
          content=response.text
          print(content)




if __name__ == '__main__':

     login=log_in()
     login.login('15565122198')




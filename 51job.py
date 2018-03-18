#coding:utf-8



import os

from pprint import pprint

import csv

from collections import Counter



from bs4 import BeautifulSoup

import requests



import jieba





class JobSpider:

    """

    51 job 网站爬虫类

    """



    def __init__(self):

        self.company = []

        self.text = ""

        self.headers = {

            'X-Requested-With': 'XMLHttpRequest',

            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'

                          '(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'

        }



    def job_spider(self):

        """ 爬虫入口

        """

        url = "http://search.51job.com/list/010000%252C020000%252C030200%252C040000,000000,0000,00,9,99,Python%2B%25E7%2588%25AC%25E8%2599%25AB,2,{}.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=1&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=" 

     

        urls = [url.format(p) for p in range(1,14)]

        
        for url in urls:
                 print(url)
           
                 r = requests.get(url, headers=self.headers).content.decode('gbk')

                 bs = BeautifulSoup(r, 'lxml').find(

                      "div", class_="dw_table").find_all("div", class_="el")
                 try:
                     for b in bs:

                     

                          href, post = b.find('a')['href'], b.find('a')['title']

                          locate = b.find('span', class_='t3').text

                          salary = b.find('span', class_='t4').text

                          d = {

                               'href': href,

                              'post': post,

                              'locate': locate,

                              'salary': salary

                              }

                          self.company.append(d)

                 except Exception:

                     pass










    def post_counter(self):

        """ 职位统计

        """

        lst = [c.get('post') for c in self.company]

        counter = Counter(lst)

        counter_most = counter.most_common()

        pprint(counter_most)

        with open(os.path.join("data", "post_pre_counter.csv"),

                  "w+", encoding="utf-8") as f:

            f_csv = csv.writer(f)

            f_csv.writerows(counter_most)



    def post_salary_locate(self):

        """ 招聘大概信息，职位，薪酬以及工作地点

        """

        lst = []

        for c in self.company:

            lst.append((c.get('salary'), c.get('post'), c.get('locate')))

        pprint(lst)

        file_path = os.path.join("data", "post_salary_locate.csv")

        with open(file_path, "w+", encoding="utf-8") as f:

            f_csv = csv.writer(f)

            f_csv.writerows(lst)



    @staticmethod

    def post_salary():

        """ 薪酬统一处理

        """

        mouth = []

        year = []

        thousand = []

        with open(os.path.join("data", "post_salary_locate.csv"),

                  "r", encoding="utf-8") as f:

            f_csv = csv.reader(f)

            for row in f_csv:
                 try:

                     if "万/月" in row[0]:

                         mouth.append((row[0][:-3], row[2], row[1]))
                 except:
                     pass
                 try:
                    if  "万/年"  in row[0]:
                         year.append((row[0][:-3], row[2], row[1]))
                 except:
                     pass

                 try:
                     if  "千/月" in row[0]:

                        thousand.append((row[0][:-3], row[2], row[1]))
                 except:
                     pass

        # pprint(mouth)



        calc = []

        for m in mouth:

            s = m[0].split("-")

            calc.append(

                (round(

                    (float(s[1]) - float(s[0])) * 0.4 + float(s[0]), 1),

                 m[1], m[2]))

        for y in year:

            s = y[0].split("-")

            calc.append(

                (round(

                    ((float(s[1]) - float(s[0])) * 0.4 + float(s[0])) / 12, 1),

                 y[1], y[2]))

        for t in thousand:

            s = t[0].split("-")

            calc.append(

                (round(

                    ((float(s[1]) - float(s[0])) * 0.4 + float(s[0])) / 10, 1),

                 t[1], t[2]))

        pprint(calc)
        with open(os.path.join("data", "post_salary.csv"),

                  "w+", encoding="utf-8") as f:

            f_csv = csv.writer(f)

            f_csv.writerows(calc)








    @staticmethod

    def insert_into_db():

        """ 插入数据到数据库

            create table jobpost(

                j_salary float(3, 1),

                j_locate text,

                j_post text

            );

        """

        import pymysql

        conn = pymysql.connect(host="localhost",

                               port=3306,

                               user="root",

                               passwd="",

                               db="mysql",

                               charset="utf8")

        cur = conn.cursor()

        with open(os.path.join("data", "post_salary.csv"),

                  "r", encoding="utf-8") as f:

            f_csv = csv.reader(f)

            sql = "insert into jobpost1(j_salary, j_locate, j_post) values(%s, %s, %s)"

            for row in f_csv:
                 try:

                     value = (row[0], row[1], row[2])
                 except:
                     pass

                 try:
                     cur.execute('CREATE TABLE IF NOT EXISTS jobpost1(j_salary REAL,j_locate TEXT,j_post TEXT)')
                     cur.execute(sql, value)

                     conn.commit()

                 except Exception as e:

                     print(e)

        cur.close()





if __name__ == "__main__":

    spider = JobSpider()

    spider.job_spider()

    # 按需启动

    spider.post_salary_locate()

    #spider.post_salary()

    #spider.insert_into_db()

 





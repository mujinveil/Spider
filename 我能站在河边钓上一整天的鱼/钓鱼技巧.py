#coding=utf-8
import requests
from lxml import etree
import pymysql 
import pymongo

client=pymongo.MongoClient('localhost')
db=client['fishing']
collections=db['fish_technique']

class mysqldata(object):
      def __init__(self):
         try:
             self.db=pymysql.connect(host='localhost',user='root',password='',port=3306,db='mysql',charset='utf8')
             self.cursor=self.db.cursor()
         except pymysql.MySQLError as e:
             print(e.args)


      def create_table(self):
          sql='CREATE TABLE IF NOT EXISIS diaoyu (id  INT PRIMARY KEY AUTO_INCREMENT not null,title varchar(255),date datetime,author varchar(20),viewed varchar(50),comment_num varchar(50),summary varchar(255),content longtext'
          self.cursor.execute(sql)
          self.db.commit()


      def insert(self,data):
         keys=','.join(data.keys)
         values=','.join(['%s']*len(data))
         sql_query='insert into diaoyu (%s) values(%s)' %(keys,values)
         try:
            self.cursor.execute(sql_query,tuple(data.values()))
            self.db.commit()
         except pymysql.MySQLError as e:
            print(e.args)
            self.db.rollback()


            

class crawl_jiqiao(object):
      def __init__(self):
          self.headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Cookie': 'KKOM_b949_saltkey=E9cDg2XR; KKOM_b949_lastvisit=1532757396; KKOM_b949_CITYPINYIN=shenzhen; KKOM_b949_440300_airpressure=1007; KKOM_b949_lastact=1532761057%09portal.php%09diaochang; Hm_lvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075; Hm_lpvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075',
                        'Host': 'www.haodiaoyu.com',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
     
      
      def get_response(self,url):
         response=requests.get(url=url,headers=self.headers).text
         content=etree.HTML(response)

         return content


      def total_num(self,base_url):

          tree=self.get_response(url=base_url)
          total_page=int(tree.xpath('//a[@class="last"]/text()')[0].replace('...','').strip())
          return total_page



      def get_links(self,base_url,page):
           url=base_url+'?page={0}'.format(page)
           tree=self.get_response(url=url)
           links=tree.xpath('//dt[@class="xs2"]/a[@target="_blank"]/@href')

           return links

      def get_content(self,link):
           print('---正在抓取---',link)
           tree=self.get_response(url=link)
           item={}
           item['title']=tree.xpath('//div[@class="h hm"]/h1/text()')[0]
           item['date']=tree.xpath('//div[@class="h hm"]/p/text()')[0].strip()
           try:
              item['author']=tree.xpath('//div[@class="h hm"]/p/a/text()')[0]
           except:
              item['author']=''
           item['viewed']=tree.xpath('//div[@class="h hm"]/p/em/text()')[0]
           item['comment_num']=tree.xpath('//div[@class="h hm"]/p/text()')[-1].strip()
           item['summary']=tree.xpath('//div[@class="s"]/div/text()')[0]
           item['content']=''.join(tree.xpath('//td[@id="article_content"]/p/text()'))
           return item
 
def main(base_url):              
    crawl=crawl_jiqiao()

    total_page=crawl.total_num(base_url)
    #mysql=mysqldata()
    #mysql.create_table()

    for i in range(total_page):
         i=i+1
         links=crawl.get_links(base_url,i)
         for link in links:
             item=crawl.get_content(link)
             #mysql.insert(item)
             print('开始注入数据')
             collections.insert(dict(item))



if __name__ == '__main__':
   bases_url=['http://www.haodiaoyu.com/jiqiao/tiaopiao/','http://www.haodiaoyu.com/jiqiao/siji/','http://www.haodiaoyu.com/jiqiao/diaofa/']
   for base_url in bases_url:
      main(base_url)



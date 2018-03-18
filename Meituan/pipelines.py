# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import pymysql
import chardet

class MeituanPipeline(object):

    def __init__(self):
        self.filename = open("cinema.json", "wb")

    def process_item(self, item, spider):
        text = json.dumps(dict(item), ensure_ascii = False) + ",\n"
        self.filename.write(text.encode("utf-8"))
        return item

    def close_spider(self, spider):
        self.filename.close()





class MysqlPipelene(object):
     def __init__(self):
         self.conn = pymysql.connect(host = 'localhost', port = 3306, user = 'root',
                passwd = 'zhouyu560609', db = 'mysql', charset = 'utf8')
         self.cur = self.conn.cursor()
     def process_item(self, item, spider):
         self.cur.execute('CREATE TABLE IF NOT EXISTS  hoteldj (city Text,hotel_name Text,hotel_address Text,hotel_price REAL,avg_score REAL,decoration_date Text,open_date TEXT,phone_number TEXT,longitude REAL,latitude REAL)')
         self.cur.execute('INSERT INTO hoteldj (city,\
                                       hotel_name, \
         	                           hotel_address,\
         	                           hotel_price, \
         	                           avg_score,\
         	                           decoration_date,\
         	                           open_date,\
         	                           phone_number,\
         	                           longitude,\
         	                           latitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (item['city'].encode('utf-8'), item['hotel_name'].encode('utf-8'), item['hotel_address'].encode('utf-8'),item['hotel_price'],item['avg_score'], item['decoration_date'].encode('utf-8'), item['open_date'].encode('utf-8'),item['phone_number'].encode('utf-8'),item['longitude'],item['latitude']))
         self.conn.commit()
         return item

     def close_spider(self, spider):
         self.cur.close()
         self.conn.close()
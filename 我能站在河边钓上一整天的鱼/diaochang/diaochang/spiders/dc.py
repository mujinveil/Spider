# -*- coding: utf-8 -*-
import scrapy
from diaochang.items import DiaochangItem
from scrapy import Request
from  diaochang.cities import get_cities

city_names=get_cities()


class DcSpider(scrapy.Spider):
    name = "dc"
    allowed_domains = ["www.haodiaoyu.com"]
    headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
     'Accept-Encoding':'gzip, deflate',
     'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
     'Cache-Control': 'max-age=0',
     'Connection': 'keep-alive',
     'Cookie': 'KKOM_b949_saltkey=E9cDg2XR; KKOM_b949_lastvisit=1532757396; KKOM_b949_CITYPINYIN=shenzhen; KKOM_b949_440300_airpressure=1007; KKOM_b949_lastact=1532761057%09portal.php%09diaochang; Hm_lvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075; Hm_lpvt_33b44eaa652bf7aa2fca0c2031abe3ba=1532761075',
     'Host': 'www.haodiaoyu.com',
     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
    
    def start_requests(self):
         for city in city_names:
              url='http://www.haodiaoyu.com/diaochang/{0}'.format(city)
              yield Request(url=url,headers=self.headers,callback=self.parse)


    def parse(self, response):
        #总页数
        total_page=int(response.xpath('//div[@id="pageNav"]/@data-total').extract_first())
        for i in range(total_page):
            i=i+1
            url=response.url+'?page='+str(i)
            yield Request(url=url,headers=self.headers,callback=self.parse_content)

    def parse_content(self,response):

        item=DiaochangItem()
        nodes=response.xpath('//div[@class="right-info"]')
        for node in nodes:
            item['name']=node.xpath('.//div[1]/text()').extract_first()
            item['score']=node.xpath('.//p[@class="score"]/text()').re('\d分')[0]
            item['address']=node.xpath('.//p[@class="desc"][1]/text()').extract_first()
            item['price']=node.xpath('.//p[@class="desc"][2]/text()').extract_first()
            yield item



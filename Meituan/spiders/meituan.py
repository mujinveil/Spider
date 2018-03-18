# -*- coding: utf-8 -*-
import scrapy
from  pypinyin import lazy_pinyin
from Meituan.items import MeituanItem

class MeituanSpider(scrapy.Spider):
    name = 'meiTuan'
    allowed_domains = ['meituan.com']
    s_url = 'http://hotel.meituan.com/'
    def get_header(self):
         headers={
                'Host': 'hotel.meituan.com',
                #Proxy-Connection: keep-alive
                'Upgrade-Insecure-Requests': '1',
                #User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding':'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
         }

    def get_cookie(self):
        cookie='_lxsdk_cuid=1620f1cbbddc8-08b5f4a6fd4118-3b60450b-100200-1620f1cbbdec8; _lxsdk=1620f21e8496c-0e3c3034615004-3b60450b-100200-1620f21e84bc8; Hm_lvt_f66b37722f586a240d4621318a5a6ebe=1520686234; oc=0HrpfKnrPdb5KKZ4xnvhl2WgzgU5ROWLIuJ7sJaPRl9GKhz-eA2o8t-H6R8rYIgKwSgmDvWCZdGgn1XIAVA5_yO4pImOmV4GyrQPFdcsce80hhUcToZNDwangXHOP9ChtbV54QU7WsHHtQqEcc4h4xu4FeA1ofuxoA7OCra9P94; rvct=400; iuuid=13796CDF513A0D33D3794CC31FC1A3C6E46B4953555EB197D7A6F54D7BA20E63; webp=1; __utma=74597006.588260647.1520939085.1520939085.1520939085.1; __utmz=74597006.1520939085.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); ci=45; cityname=%E9%87%8D%E5%BA%86; mtcdn=K; _hc.v=14297769-46da-af3a-a31d-fc99968581f4.1520939603; latlng=22.54286,114.05956,1520940828935; i_extend=E191459305076700592527314237998911834833_v973522682177995096Gimthomepagecategory122H__a100005__b1; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; uuid=b497eab5bef84496902e.1520985399.1.0.0; __mta=146076939.1520671964610.1520941188155.1520986817342.11; hotel_city_id=1; hotel_city_info=%7B%22id%22%3A1%2C%22name%22%3A%22%E5%8C%97%E4%BA%AC%22%2C%22pinyin%22%3A%22beijing%22%7D; __mta=146076939.1520671964610.1520986817342.1520991006854.12; _lxsdk_s=%7C%7C1'
        arr = cookie.replace(' ', '').split(';')
        cookies = {}
        for string in arr:
             string = string.split('=')
             cookies[string[0]] = string[1]
             return cookies 
    '''
    def start_requests(self):
         cookies = self.get_cookie()
         headers = self.get_header()
         url = 'http://www.meituan.com/changecity/'
         yield scrapy.Request(url = url, headers = headers, cookies = cookies, callback = self.parse_city,dont_filter=True)
    '''
    def start_requests(self):

         #cities = response.xpath("//div[@class='city-area']/span[@class='cities']/a/text()").extract()
         headers = self.get_header()
         cookies = self.get_cookie()
         city='shenzhen'
         for i in range(52):
             url=self.s_url+city+'/pn'+str(i)
             yield scrapy.Request(url=url,headers=headers,cookies=cookies,callback=self.parse,dont_filter=True)




    def parse(self, response):
         
        
        #next_url=response.xpath('//div[@class="paginator-wrapper"]/ul/li[@class="page-link"]/a/@href').extract()
        hotel_url=response.xpath('//article[@class="poi-item"]/div[@class="info-wrapper"]/h3/a/@href').extract()
        headers = self.get_header()
        cookies = self.get_cookie()
        for url in hotel_url:
            scrapy.Request(url=url,headers=headers,cookies=cookies,callback=self.parse_hotel,dont_filter=True)

        #for next_page  in next_url:
             #yield scrapy.Request(url=next_page,headers=headers,cookies=cookies,callback=self.parse)

    def parse_hotel(self,response):
         item=MeituanItem()
         item['city']=response.xpath('//div[@class="site-wrapper-wide"]//div[@class="breadcrumb-nav"]/a/text()').extract()[0]
         item['hotel_name']=response.xpath('//div[@class="relative clear"]/span/text()').extract()[0]
         item['hotel_address']=response.xpath('//div[@class="fs12 mt6 mb10"]/span/text()').extract()[0]
         price=response.xpath('//div[@class="pull-right"]/div[@class="price-color"]/em/text()').extract()
         if price:
             item['hotel_price']=price[0]
         else:
             item['hotel_price']=0
         item['avg_score']=response.xpath('//div[@class="other-detail-line1-score"]/span/text()').extract()[0]
         item['decoration_date'],item['open_date']=response.xpath('//div[@class="mb10 ellipsis"]/span/text()').extract()
         item['phone_number']=response.xpath('//div[@class="mb10"]/text()').extract()[0].split('：')[1]
         zuobiao=response.xpath('//img[@alt="地图"]/@src').extract()[0]
         if len(zuobiao) !=0:
             item['longitude']=zuobiao.split('|')[1].split(',')[0]
             item['latitude'] =zuobiao.split('|')[1].split(',')[1]

         yield item
# -*- coding: utf-8 -*-
import scrapy
from ganji.items import GanjiItem
from scrapy import Request


class GjSpider(scrapy.Spider):
    name = 'gj'
    allowed_domains = ['sz.ganji.com']
    start_urls = ['http://sz.ganji.com/fang1/']

    def parse(self, response):
        item=GanjiItem()
        nodes=response.xpath('//dl[@class="f-list-item-wrap f-clear"]')
        for house in nodes:
            item['name']=house.xpath('.//dd[@class="dd-item title"]/a/text()').extract_first()
            item['area']=house.xpath('.//dd[@class="dd-item size"]/@data-area').extract_first()
            item['address']=house.xpath('.//dd[@class="dd-item address"]/span/a[1]/text()').extract_first()
            item['money']=house.xpath('.//dd[@class="dd-item info"]//span[1]/text()').extract_first()
            yield item
        np=response.xpath('//a[@class="next"]/@href').extract_first()
        fullurl='http://sz.ganji.com'+str(np)
        yield Request(url=fullurl,callback=self.parse)
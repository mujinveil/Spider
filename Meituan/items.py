# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MeituanItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

     city=scrapy.Field()
     hotel_name=scrapy.Field()
     hotel_address=scrapy.Field()
     hotel_price=scrapy.Field()
     avg_score=scrapy.Field()
     decoration_date=scrapy.Field()
     open_date=scrapy.Field()
     phone_number=scrapy.Field()
     longitude=scrapy.Field()
     latitude=scrapy.Field()

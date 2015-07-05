# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SatItem(scrapy.Item):
    """Sentinel Item in JSON naming convention"""
    metadata = scrapy.Field()
    download = scrapy.Field()
    footprint = scrapy.Field()
    id = scrapy.Field()
    identifier = scrapy.Field()
    requestId = scrapy.Field()



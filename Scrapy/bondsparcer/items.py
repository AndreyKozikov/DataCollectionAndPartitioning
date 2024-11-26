# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BondsparcerItem(scrapy.Item):
    name = scrapy.Field()
    nominal = scrapy.Field()
    price = scrapy.Field()
    nkd = scrapy.Field()
    cost = scrapy.Field()
    coupon = scrapy.Field()
    rate = scrapy.Field()
    profitability = scrapy.Field()


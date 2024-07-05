import re
import scrapy
import logging
from scrapy.spiders import SitemapSpider

class MySitemapSpider(SitemapSpider):
    name = 'contorion_spec'
    sitemap_urls = ['https://www.contorion.de/pdp_index.xml']

    def parse(self, response):
        gtin = response.xpath('//div[@id="product-description"]//p[contains(text(), "EAN/GTIN")]/text()').get().split(': ')[1]
        gtin = re.sub('\\s+', ' ', gtin).strip()


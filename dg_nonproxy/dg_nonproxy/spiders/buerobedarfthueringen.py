import scrapy
from scrapy.http import XmlResponse
import scrapy.selector
from scrapy.spiders import SitemapSpider
from dg_nonproxy.items import SpecItem

class Buerobedarfthueringen(SitemapSpider):
    name = 'buerobedarfthueringen'
    sitemap_urls = ['https://www.buero-bedarf-thueringen.de/1_index_sitemap.xml']


    def _parse_sitemap(self, response):
        if isinstance(response, XmlResponse) and response.xpath('//sitemap'):
            # Dies ist eine Sitemap-Indexdatei, also verarbeiten wir sie wie gewohnt
            for req in super()._parse_sitemap(response):
                yield req
        elif isinstance(response, XmlResponse) and response.xpath('//urlset'):
            # Dies ist eine einzelne Sitemap, also extrahieren wir die URLs und Prioritäten
            s = scrapy.selector.Selector(response)
            urls = s.xpath('//url/loc/text()').getall()
            priorities = s.xpath('//url/priority/text()').getall()
            for url, priority in zip(urls, priorities):
                if float(priority) == 0.9:
                    yield scrapy.Request(url, self.parse)
        else:
            for req in super()._parse_sitemap(response):
                yield req
    
    def parse(self, response):
        gtin = response.xpath('//meta[@itemprop="gtin13"]/@content').get(default="")
        if not gtin:
            return        
        table = response.xpath('//table[@class="table table-striped"]/..//tr')
        if not table:
            return
        item = SpecItem()
        for row in table:
            item['providerKey'] = gtin
            specKey = row.xpath('.//td/label/.//text()').getall()
            item['SpecificationKey'] = ' '.join([v.strip() for v in specKey if v.strip()]).removesuffix(':')
            specValue = row.xpath('.//td[2]//text()').getall()
            item['SpecificationValue'] = ' '.join([v.strip() for v in specValue if v.strip()])
            yield item





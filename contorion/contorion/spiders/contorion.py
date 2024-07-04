import re
import scrapy
from scrapy.spiders import SitemapSpider
from datetime import datetime
import pyodbc
import json
from contorion.items import ProductItem, SpecItem

class MySitemapSpider(SitemapSpider):
    name = 'contorion'
    sitemap_urls = ['https://www.contorion.de/pdp_index.xml']
    sitemap_rules = [
        ("/prebena","parse"),
        ("/bosch","parse"),
        ("/dunlop","parse"),
        ("/hoppe","parse"),
        ("/husqvarna","parse"),
        ("/makita","parse")
    ]
    def __init__(self, *a, **kw):
        super(MySitemapSpider,self).__init__(*a, **kw)
        systemgtins = set()
        conn = pyodbc.connect('DSN=Big Query dg-dp-bqondemand-prod', autocommit=True)
        cursor = conn.cursor()
        cursor.execute("SELECT distinct  ProviderGtin FROM `dg-dp-dwhdata-prod.cm.biz_AssortmentAndConflictForApproval` ass  WHERE ass.ConflictGroup1Code = 2 AND ProviderBrand IN ('Bosch Professional','Bosch Automotive','Bosch Diagnostics','Bosch Home & Garden','Bosch Zubehör','	Bosch Hausgeräte','Bosch Smart Home','Bosch Home Comfort','Dunlop','Hoppe','Husqvarna','Makita')")
        rows = cursor.fetchall()
        for row in rows:
            systemgtins.add(row[0])
        self.sysgtin = systemgtins

    def parse(self, response):
        gtin = response.xpath('//div[@id="product-description"]//p[contains(text(), "EAN/GTIN")]/text()').get(default="")
        if not gtin:
            return
        gtin = gtin.split(': ')[1]
        gtin = re.sub('\s+', ' ', gtin).strip()    
        if gtin not in self.sysgtin:
            return
        description = response.css('#product-description > div.t-panel__body.m-collapsible__target.m-collapsible__target--sm.m-collapsible__target--md > div > div.js-expandable-content.t-expandable__content > ul').getall()
        description = '\n'.join(description)
        if not description:
            return
        
        ProductDataItem = ProductItem()
        ProductDataItem['gtin'] = gtin
        ProductDataItem['longdesciprtion'] = description
        ProductDataItem['title'] =  response.css('h1.a-txt--product-name::text').get().strip()
        ProductDataItem['brand'] = response.css('div.m-brand-and-product-info__brand-name a::text').get().strip()
        ProductDataItem['manufacturerkey'] = response.css('div.m-brand-and-product-info__brand-name div.a-txt--regular-plus::text').getall()[-1].strip()
        breadcrumbs = response.css('ul.breadcrumbs li.js-breadcrumbs-item span[itemprop="name"]::text').getall()
        if breadcrumbs and breadcrumbs[0] == 'Startseite':
            breadcrumbs = breadcrumbs[1:]
        ProductDataItem['productType'] = '>'.join(breadcrumbs)
        yield ProductDataItem
        
        #artno = response.xpath('//*[@id="content"]/div[3]/div[1]/div[2]/div/div[4]/div/div[1]/div[2]/div[2]/span/text()').get()
        artno = re.search(r'(\d+)$',response.url)
        artno = artno.group(1)
        if not gtin:
            return
        ajax_url = f'https://www.contorion.de/catalog/lazy-detail/display-attributes/{artno}'
        headers = {
            "accept": "*/*",
            "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "sec-ch-ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest"
        }

        yield scrapy.Request(ajax_url, callback=self.parse_ajax, headers=headers, cb_kwargs={"gtin":gtin}, errback=self.errback)

    def parse_ajax(self, response, gtin):    
        item = SpecItem()
        table = response.css('#product-details > div.t-panel__body.t-panel__body--unpadded.m-collapsible__target.m-collapsible__target--sm.m-collapsible__target--md > div > div.js-expandable-content.t-expandable__content > table > tbody > tr')
        for row in table:
            item['providerKey'] = gtin
            item['SpecificationKey'] = row.css('td:nth-child(1)::text').get()
            item['SpecificationValue'] = row.css('td:nth-child(2)::text').get()
            yield item

    def errback(self, failure):
        print(failure.request.meta)

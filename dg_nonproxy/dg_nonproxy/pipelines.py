# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dg_noproxy.items import ProductItem, SpecItem, MediaItem
import paramiko
from paramiko import Transport, SFTPClient
import csv

class BasePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        spider = crawler.spider
        return cls(spider)

    def __init__(self, spider):
        self.spider = spider
        self.filename = None
        self.file = None
        self.writer = None
        self.data = []

    def open_spider(self, spider):
        self.filename = f'{self.spider.name}_Scrapy_{self.spider.file_format}.csv'
        self.file = open(self.filename, 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";")
        headerkeys = list(self.spider.item_type.fields.keys())
        self.writer.writerow(headerkeys)


class SpecPipeline(BasePipeline):
    def __init__(self, spider):
        super().__init__(spider)
        self.spider.item_type = SpecItem
        self.spider.file_format = 'spec'


class ProductPipeline(BasePipeline):
    def __init__(self, spider):
        super().__init__(spider)
        self.spider.item_type = ProductItem
        self.spider.file_format = 'master'


class MediaPipeline(BasePipeline):
    def __init__(self, spider):
        super().__init__(spider)
        self.spider.item_type = MediaItem
        self.spider.file_format = 'media'

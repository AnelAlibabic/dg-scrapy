# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dg_nonproxy.items import ProductItem, SpecItem, MediaItem
import paramiko
from paramiko import Transport, SFTPClient
import csv
import os

class BasePipeline:
    
    def close_spider(self, spider):
        if not self.data:
            self.file.close()
            os.remove(self.filename)
            return
        self.data.sort(key=lambda x: x[0])  # sort by providerkey
        for row in self.data:
            self.writer.writerow(row)
        self.file.close()
        transport = Transport(('ftp.digitec.ch', 22))
        transport.connect(username='pdi_management', password='y59HVFJuyZiXco9f28Bh')
        sftp = SFTPClient.from_transport(transport)
        file = open(self.filename, 'rb')
        sftp.put(self.filename, f'/Anel/{self.filename}')
        file.close()
        sftp.close()
        transport.close()
      #  os.remove(self.filename)


class SpecPipeline(BasePipeline):
    def open_spider(self, spider):
        self.data = []
        self.filename = f'{spider.name}_Scrapy_spec.csv'
        self.file = open(self.filename, 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";")
        headerkeys = list(SpecItem.fields.keys())
        self.writer.writerow(headerkeys)

    
    def process_item(self, item, spider):
        if isinstance(item, SpecItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            if row not in self.data:  # Duplikate vermeiden
                self.data.append(row)  # convert list to tuple and add to list
        return item
    
class ProductPipeline(BasePipeline):
    def open_spider(self, spider):
        self.data = []
        self.filename = f'{spider.name}_Scrapy_master.csv'
        self.file = open(self.filename, 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";", quoting=csv.QUOTE_ALL)
        headerkeys = list(ProductItem.fields.keys())
        self.writer.writerow(headerkeys)


    def process_item(self, item, spider):
        if isinstance(item, ProductItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            if row not in self.data:  # Duplikate vermeiden
                self.data.append(row)  # convert list to tuple and add to list
        return item
    
class MediaPipeline(BasePipeline):
    def open_spider(self, spider):
        self.data = []
        self.filename = f'{spider.name}_Scrapy_media.csv'
        self.file = open(self.filename, 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";", quoting=csv.QUOTE_ALL)
        headerkeys = list(MediaItem.fields.keys())
        self.writer.writerow(headerkeys)

    def process_item(self, item, spider):
        if isinstance(item, MediaItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            if row not in self.data:  # Duplikate vermeiden
                self.data.append(row)  # convert list to tuple and add to list
        return item
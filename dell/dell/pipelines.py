# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dell.items import ProductItem, SpecItem
import csv
import os
import paramiko
from paramiko import Transport, SFTPClient

class DellPipeline:
    def process_item(self, item, spider):
        return item
class SpecPipeline:
    def open_spider(self, spider):
        self.data = []
        self.file = open('dell_Scrapy_spec.csv', 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";")
        headerkeys = list(SpecItem.fields.keys())
        self.writer.writerow(headerkeys)

    def close_spider(self, spider):
        self.data.sort(key=lambda x: x[0])  # sort by providerkey
        for row in self.data:
            self.writer.writerow(row)
        self.file.close()

        transport = Transport(('ftp.digitec.ch', 22))  # Port 22 ist der Standardport für SFTP
        transport.connect(username='pdi_management', password='y59HVFJuyZiXco9f28Bh')
        sftp = SFTPClient.from_transport(transport)
        file = open('dell_Scrapy_spec.csv', 'rb')
        sftp.put('dell_Scrapy_spec.csv', '/Anel/dell_Scrapy_spec.csv')
        file.close()
        sftp.close()
        transport.close()

    def process_item(self, item, spider):
        if isinstance(item, SpecItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            if row not in self.data:  # Duplikate vermeiden
                self.data.append(row)  # convert list to tuple and add to list
        return item
    
class ProductPipeline:
    def open_spider(self, spider):
        self.file = open('dell_Scrapy_master.csv', 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";", quoting=csv.QUOTE_ALL)
        headerkeys = list(ProductItem.fields.keys())
        self.writer.writerow(headerkeys)

    def close_spider(self, spider):
        self.file.close()
        transport = Transport(('ftp.digitec.ch', 22))
        transport.connect(username='pdi_management', password='y59HVFJuyZiXco9f28Bh')
        sftp = SFTPClient.from_transport(transport)
        file = open('dell_Scrapy_master.csv', 'rb')
        sftp.put('dell_Scrapy_master.csv', '/Anel/dell_Scrapy_master.csv')
        file.close()
        sftp.close()
        transport.close()

    def process_item(self, item, spider):
        if isinstance(item, ProductItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            self.writer.writerow(row)
        return item
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from contorion.items import ProductItem, SpecItem
import csv


class ContorionPipeline:
    def process_item(self, item, spider):
        return item


class SpecPipeline:
    def open_spider(self, spider):
        self.file = open('spec.csv', 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";", quoting=csv.QUOTE_ALL)
        headerkeys = list(SpecItem.fields.keys())
        self.writer.writerow(headerkeys)

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if isinstance(item, SpecItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            self.writer.writerow(row)
        return item
    
class ProductPipeline:
    def open_spider(self, spider):
        self.file = open('product.csv', 'w', newline='', encoding= 'utf-8')
        self.writer = csv.writer(self.file, delimiter=";", quoting=csv.QUOTE_ALL)
        headerkeys = list(ProductItem.fields.keys())
        self.writer.writerow(headerkeys)

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if isinstance(item, ProductItem):
            row = [item.get(key, "") for key in item.fields.keys()]
            self.writer.writerow(row)
        return item

                
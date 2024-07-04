import json
import scrapy
import pyodbc
import requests
import logging
import w3lib.html
import time
from scrapy.selector import Selector

from acer.items import ProductItem, SpecItem

class AcerSpider(scrapy.Spider):
    name = 'acer'
    token = None
    token_time = None
    token_expires_in = None

    def __init__(self, *a, **kw):
        super(AcerSpider,self).__init__(*a, **kw)
        systemgtins = set()
        conn = pyodbc.connect('DSN=Big Query dg-dp-bqondemand-prod', autocommit=True)
        cursor = conn.cursor()
        cursor.execute("SELECT distinct SubString(ProviderGtin,2) FROM `dg-dp-dwhdata-prod.cm.biz_AssortmentAndConflictForApproval` ass JOIN `dg-dp-dwhdata-prod.cm.biz_Product` p ON  ass.ProductId = p.ProductId WHERE p.BrandId = 19")
        rows = cursor.fetchall()
        for row in rows:
            systemgtins.add(row[0])
        self.sysgtin = systemgtins

    def start_requests(self):
        self.token = self.get_token()
        self.token_time = time.time()
        self.token_expires_in = self.token['expires_in'] - 300  # Renew the token 5 minutes before it actually expires
        for gtin in self.sysgtin:
            if not gtin:
                return
            if time.time() - self.token_time >= self.token_expires_in:  
                self.token = self.refresh_token(self.token['refresh_token'])
                self.token_time = time.time()
            access_token = self.token['access_token']
            url = 'https://acer-pim.de/rest/v3/request/acer/rest/getProductAttributes/?query='+gtin+"&language=de"
            headers = {
            'Authorization': f'Bearer {access_token}' ,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
            yield scrapy.Request(url, method='GET',headers=headers,cb_kwargs={'gtin':gtin,'token':self.token}, callback=self.parse)

    def get_token(self):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data={'grant_type': 'password', 'client_id': '1','client_secret':'clientSecret','username':'anel.alibabic@digitecgalaxus.ch','password':'Ur=J?txtIC7\\DU^~LvI2NQU;R@CG3*'}
        response = requests.post('https://acer-pim.de/rest/v3/request/join/serverRest/accessToken/',data=data,headers=headers)
        data = response.json()
        return data['body']

    def refresh_token(self, refresh_token):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data={'grant_type': 'refresh_token', 'client_id': '1','client_secret':'clientSecret','refresh_token':refresh_token}
        response = requests.post('https://acer-pim.de/rest/v3/request/join/serverRest/accessToken/',data=data,headers=headers)
        data = response.json()
        return data['body']

    def parse(self, response,gtin,token):
        data = json.loads(response.text)
        if 'error' in data.get('message'):
            logging.warning(f'Message: {data.get("message")}')
            self.token = self.refresh_token(token['refresh_token'])
            self.token_time = time.time()
            access_token = self.token['access_token']
            yield response.request.replace(headers={'Authorization': f'Bearer {access_token}'})
        if 'Sku not found:' in data.get('body'):
            logging.warning(f'SKU not found for GTIN {gtin}')
            return

        product_item = ProductItem()
        product_item['providerkey'] = data['body']['general']['acerPimOutputAcerPN']
        product_item['gtin'] = gtin
        product_item['title'] = data['body']['general']['acerPimOutputHeadline']
        yield product_item

        for group in data['body']['groups']:
            for entry in group['entries']:
                spec_item = SpecItem()
                spec_item['providerKey'] = data['body']['general']['acerPimOutputAcerPN']
                spec_item['SpecificationKey'] = entry['label']
                spec_item['SpecificationValue'] = entry['value']
                yield spec_item

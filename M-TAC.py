import sqlite3
from requests import Session
import requests
from lxml import html
from lxml.html import HtmlElement
import time
from datetime import date
import datetime
import asyncio
from functools import partial
import random
import sys
import logging
import functools
import pickle
import os.path
from fp.fp import FreeProxy
import random
from chrome_version import Chrome
import pandas as pd
import psycopg2
import connect
from config import load_config
import json
from json import dumps, loads
s = Session()

url = 'https://militarist.ua/ua/'

conn = psycopg2.connect(**load_config())


try:
    config = load_config()
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DROP TABLE mtak;
        """)
            cur.execute("""
        CREATE TABLE IF NOT EXISTS mtak(
            id SERIAL PRIMARY KEY,
            time timestamp NOT NULL,
            name VARCHAR(250) NOT NULL,
            price INTEGER NOT NULL,
            link VARCHAR(250) NOT NULL
        )
        """)
except (psycopg2.DatabaseError, Exception) as error:
    print(error)


if os.path.exists('root.bin'):
        with open('root.bin', 'rb') as my_file:
            try:
               load_data = pickle.load(my_file)
               room = load_data
               
               #print(root)
            except pickle.UnpicklingError:
                print('error')

else:
    print('root.bin isn`t')
    room = Chrome()


proxies = FreeProxy(country_id=['US', 'BR'], timeout=10, rand=True).get_proxy_list(1)



def get_proxy_response(url):
    headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.choice(room)} Safari/537.36 Edg/125.0.0.0'}  #{random.choice(room)}
            
    
    for proxy in proxies:

        response = requests.get(url=url,proxies={'http': proxy}, headers=headers)
        #print(response.status_code, proxy) 

        if response.status_code == 200:
            break
    return response


        


class Category(object):
    def __init__(self, name):
        self.name = name
        self.products = []
        self.subgroups = []
        self.link = None
        self.endpoint = None
        self.response = None
        self.all_href = []

    def showall(self):
        print(f'name {self.name}, link {self.link} ')
        if len(self.subgroups) != 0:
            for g in self.subgroups:
                g.showall()

    async def get_responses(self):
        
        endpoints = []
        self.get_endpoints(endpoints)
        tasks = []
        for endpoint in endpoints:
            task = asyncio.create_task(endpoint.get_response())
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def get_response(self):
        loop = asyncio.get_event_loop()
        if self.response is None:
            #print(self.link , self.name)
            self.response = await loop.run_in_executor(None, partial(get_proxy_response, url=self.link))
 
    def get_endpoints(self, endpoints: list):
         
         if len(self.subgroups) > 0:
            self.endpoint = False
            for subgroup in self.subgroups:
                subgroup.get_endpoints(endpoints)
         else:
            self.endpoint = True
            #if self.response is None:
            endpoints.append(self)

    def chek_pagination(self):
        if len(self.subgroups) != 0:
            for sub in self.subgroups:
                sub.chek_pagination()
        else:
            pagination: HtmlElement = html.fromstring(self.response.text).xpath('//div[@id="pagen-block"]//nav[@class="pagination"]//a/@href')
            for href in pagination:
                militarist_url = 'https://militarist.ua'
               
                link = militarist_url + str.strip(href)
                #print(link)
                g = Category(self.name)
                g.link = link
                g.name = self.name
                self.subgroups.append(g)

    def chek_endpoints(self):
        if len(self.subgroups) != 0:
            
            [sub.chek_endpoints() for sub in self.subgroups]
    
        else:
            
            endpoint_false: HtmlElement = html.fromstring(self.response.text).find('./div[@class="catalog-lvl1"]')

            if endpoint_false is not None:
                print(f'endpoint false in  {self.link}')
            
    def href_all_products(self):
        endpoints = []
        
        self.get_endpoints(endpoints)
     
        for endpoint in endpoints:
            #print(endpoint.response.text)
            card_product_head: HtmlElement = html.fromstring(endpoint.response.text).xpath('.//div[@class="card_product-head"]/a')
            card_product_bottom: HtmlElement = html.fromstring(endpoint.response.text).xpath('.//div[@class="card_product-bottom"]')
         
            hashes = []
            for i, href in enumerate(card_product_head):
                link = 'https://militarist.ua' + href.get('href')
                name = href.find('span').text
                hash = link.hash()
                price = card_product_bottom[i].find('.//div[@class="price"]/p[@class="price_new"]').text
                if hash not in hashes:
                    hashes.append(hash)
                    product = Product(name)
                    product.link = link
                    product.price = int(price.replace(' ', '').replace('грн.', ''))
                    product.hash = hash
                    #print(f'name: {product.name}, price: {product.price}')
                    self.products.append(product)

       
    def get_all_products(self):
        if len(self.subgroups)>0:
            for sub in self.subgroups:
                sub.get_all_products()
        if len(self.products)>0:
            name = []
            price = []
            link = []
            products = set(self.products)
            for prod in products:
                name.append(prod.name)
                price.append(prod.price)
                link.append(prod.link)

                #values = [(prod.name, prod.price, prod.link)]
                #print(values)
            config = load_config()

   
            try:
                with  psycopg2.connect(**config) as conn:
                    with  conn.cursor() as cursor:
                        # for prod in self.products:
                        #     cursor.execute("INSERT INTO mtak(name, price, link) VALUES(%s,%s,%s)", (prod.name, prod.price, prod.link))
                        values = []
                
                        
                        time = datetime.datetime.now()
                        time = time.strftime("%Y-%m-%d %H:%M:%S") 
                        for prod in self.products:
                            values.append((time, prod.name, prod.price, prod.link))
                        cursor.executemany("INSERT INTO mtak(time, name, price, link) VALUES(%s,%s,%s,%s)", values)
                        sql1 = '''select * from mtak;'''
                        cursor.execute(sql1)

                        for i in cursor.fetchall():
                            print(i)
                 
                    conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error) 
            
           

            dt = datetime.datetime.now() 
            current_datetime = dt.strftime("%Y-%m-%d %H:%M:%S") 
            table = pd.DataFrame({
        'name': [name for name in name],
        'price': [price for price in price],
        'link': [link for link in link],
        'time': current_datetime,
    })

            result = table.to_json(orient="records")
            parsed = loads(result)
       
            with open('table.json', 'w') as f:
                f.write(dumps(parsed, indent=4))

            # with open('table.json') as f:
            #     print(f.read())
            
            with pd.ExcelWriter('path_to_file.xlsx') as writer:
                table.to_excel(writer)


        
        

class Product():



    def __init__(self, name):
        self.name = name
        self.hash = None
        self.price = []
        self.link = []
        
        self.endpoint = None
        self.response = None


def reverse(htm: HtmlElement, cl: Category):
    if htm is not None:
       
        li_elements = htm.xpath('.//ul')
        
        for element in li_elements:
        # new = li.find('./li[@class="is_new"]')
            for li in element.xpath('./li'):


                militarist_url = 'https://militarist.ua'
                link = li.find('./a').get('href')
                link = militarist_url + str.strip(link)
                name = li.find('./a').text
                
                g = Category(name)
                g.link = link
                g.name = name
                cl.subgroups.append(g)
            
                ul = li.find('./ul')
                if ul is not  None:
                    reverse(ul, g)
                else:
                    continue


    
async def main():
    start_time = time.monotonic()
    if os.path.exists('second_responses.pkl'):
        with open('second_responses.pkl', 'rb') as my_file:
            try:
                load_data = pickle.load(my_file)
                root = load_data
            except pickle.UnpicklingError:
                print('error')
    else:
            
        response = get_proxy_response(url)

        for i in range(120):
            status = response.status_code
            print('response', i, status)

            if status == 200:
                break
            if i == 119:
                    print('False')
                    sys.exit()

        root = Category('root')
    
        
        start_element: HtmlElement = html.fromstring(response.text).find('.//div[@class="main_menu-block"]')
        reverse(start_element, root)
        
        await root.get_responses()
        root.chek_endpoints()
        root.chek_pagination()

        await root.get_responses()
        with open('second_responses.pkl', 'wb') as my_file:
            pickle.dump(root, my_file)

    #print(f'before href: {time.monotonic() - start_time}')
    root.href_all_products() 
    #print(f'before all_products: {time.monotonic() - start_time}')
    root.get_all_products()



    print(f'Время прошло{time.monotonic() - start_time}')
if __name__ == '__main__': 
    asyncio.run(main())

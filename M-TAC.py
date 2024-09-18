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
import asyncpg
import connect
from config import load_config
import json
from json import dumps, loads
s = Session()
import mmh3
import struct

url = 'https://militarist.ua/ua/'

conn = psycopg2.connect(**load_config())

commands = (
      
        """
            DROP TABLE IF EXISTS products CASCADE;
        """,
            """
            DROP TABLE IF EXISTS products_data CASCADE;
        """,
        """
        CREATE TABLE IF NOT EXISTS products(
            product_id BIGINT PRIMARY KEY,
            name VARCHAR(250) NOT NULL,
            link VARCHAR(250) NOT NULL
            )
        """,
         """
        CREATE TABLE IF NOT EXISTS products_data (
                data_id SERIAL PRIMARY KEY,
                product_id INTEGER,
                price INTEGER NOT NULL,
                time timestamp NOT NULL,
                FOREIGN KEY (product_id)
                REFERENCES products (product_id)
                ON UPDATE CASCADE ON DELETE CASCADE
                )
        """      
)

# try:
#     config = load_config()
#     with psycopg2.connect(**config) as conn:
#         with conn.cursor() as cur:
#             for command in commands:
#                 cur.execute(command)
# except (psycopg2.DatabaseError, Exception) as error:
#     print(error)



try:
    async def run():
        conn = await asyncpg.connect(user='postgres', password='1111',
                                     database='m-tak', host='localhost')
        for command in commands:
            await conn.execute(command)
        await conn.close()

    asyncio.run(run())

except asyncpg.exceptions.PostgresError as db_error:
    print("error of database:", db_error)
except ConnectionError as conn_error:
    print("Connection error:", conn_error)
except Exception as error:
    print("another error:", error)



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

def murmurhash_64bit(data):

    hash_value = mmh3.hash64(data)

    return struct.pack('q', hash_value[0])
        

class Category(object):
    def __init__(self, name):
        self.name = name
        self.products = []
        self.subgroups = []
        self.link = None
        self.endpoint = None
        self.response = None
        self.all_href = []
        self.__hash__ = None
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
             
                hash_object = murmurhash_64bit(link)
                # hash_8bit = hash_object.to_bytes(8, byteorder='little')
                hash_64 = int.from_bytes(hash_object, byteorder='little', signed=True)
     
                price = card_product_bottom[i].find('.//div[@class="price"]/p[@class="price_new"]').text
                if hash_64 not in hashes:
              
                    hashes.append(hash_64)
                    product = Product(name)
                    product.link = link
                    product.price = int(price.replace(' ', '').replace('грн.', ''))
                    product.hash = hash_64
                    #print(product.hash)
                    #print(f'name: {product.name}, price: {product.price}')
                    self.products.append(product)

       
    def get_all_products(self):
        if len(self.subgroups)>0:
            for sub in self.subgroups:
                sub.get_all_products()
        if len(self.products)>0 and self.products:
            name = []
            price = []
            link = []
            for prod in self.products:
                name.append(prod.name)
                price.append(prod.price)
                link.append(prod.link)

                #values = [(prod.name, prod.price, prod.link)]
                #print(values)
            config = load_config()

   
            try:
                with  psycopg2.connect(**config) as conn:
                    with  conn.cursor() as cursor:

                        values_products = []
                        values_dates = []
                
                        time = datetime.datetime.now()
                        time = time.strftime("%Y-%m-%d %H:%M:%S") 
                        for prod in self.products:
                            values_products.append((prod.hash, prod.name, prod.link))
                            values_dates.append((time, prod.price))
                        cursor.executemany("INSERT INTO products(product_id, name, link) VALUES(%s,%s,%s) ON CONFLICT (product_id) DO NOTHING;", values_products)
                        cursor.executemany("INSERT INTO products_data(time, price) VALUES(%s,%s)", values_dates)
                        #sql1 = '''select * from products_data;'''
                        #sql1 = '''select * from products;'''
                      

                        # for i in cursor.fetchall():
                        #     print(i)
                 
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



    def refresh_products(self):
      
        try:
            config = load_config()
            
            with  psycopg2.connect(**config) as conn:
                with  conn.cursor() as cursor:
                   
                    sql1 = '''select hash from products;'''
                    
                    cursor.execute(sql1)

                    a = cursor.fetchall()

                    for item in a:
                        hash_int = item[0]
                        #print(hash_int)
                        for i in self.products:

                            if hash_int == i.hash: print(f'hash in database {hash_int} and hash from site {i.hash}')
                            # if i.hash != item:
                            #     values_products = []
                            #     values_dates = []
                            #     print('here')
                            #     time = datetime.datetime.now()
                            #     time = time.strftime("%Y-%m-%d %H:%M:%S") 
                            #     update_products="UPDATE products  SET hash = %s, name = %s, link = %s"
                            #     update_products_data="UPDATE products_data  SET time = %s, price = %s"
                                
                            #     for prod in values_products: 
                            #         cursor.execute(update_products,prod) 
                            #     for prod in values_dates: 
                            #         cursor.execute(update_products_data,prod) 

                            #     for prod in self.products:
                            #         values_products.append((prod.hash, prod.name, prod.link))
                            #         values_dates.append((time, prod.price))



                                # cursor.executemany("UPDATE products SET(hash, name, link) VALUES(%s,%s,%s)", values_products)
                                # cursor.executemany("UPDATE products_data SET(time, price) VALUES(%s,%s)", values_dates)
                            


  
                        
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
                print(error) 

        
        

class Product():

    def __init__(self, name):
        self.hashes = []
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
    #root.refresh_products()
    

    print(f'Время прошло{time.monotonic() - start_time}')
if __name__ == '__main__': 

    asyncio.run(main())

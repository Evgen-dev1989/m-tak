#import sqlite3
from requests import Session
import requests
from lxml import html
from lxml.html import HtmlElement
import time
from datetime import date, datetime
import asyncio
from functools import partial
#import random
import sys
import logging
import functools
import pickle
import os.path
from fp.fp import FreeProxy
import random
import pandas as pd
import asyncpg
import json
from json import dumps, loads
import mmh3
import struct
from collections import deque
from decimal import *
from database import host, database, user, password
#import psycopg2
#import connect
#from chrome_version import Chrome
#from config import load_config
# s = Session()



#conn = psycopg2.connect(**load_config())
url = 'https://militarist.ua/ua/'


commands = (
      
        # """
        #     DROP TABLE IF EXISTS products CASCADE;
        # """,
        #     """
        #     DROP TABLE IF EXISTS products_data CASCADE;
        # """,
        """
        CREATE TABLE IF NOT EXISTS products(
            product_id BIGINT PRIMARY KEY,
            name VARCHAR(250) NOT NULL,
            link VARCHAR(250) NOT NULL
            )
        """,
         """
        CREATE TABLE IF NOT EXISTS products_data (
    data_id SERIAL,
    time TIMESTAMP NOT NULL,
    product_id BIGINT,
    price DECIMAL NOT NULL,

    FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
        """      
)


# try:
#     async def run():
#         conn = await asyncpg.connect(user='postgres', password='1111', database='m-tak', host='localhost')

#         for command in commands:
#             await conn.execute(command)
#         await conn.close()

#         asyncio.run(run())

# except asyncpg.exceptions.PostgresError as db_error:
#     print("error of database:", db_error)
# except ConnectionError as conn_error:
#     print("Connection error:", conn_error)
# except Exception as error:
    # print("another error:", error)

async def get_db_pool():
  
    return await asyncpg.create_pool(
        user=user,
        password=password,
        database=database,
        host=host
    )

async def execute_db_commands(commands, pool):

    async with pool.acquire() as conn:
        async with conn.transaction():

            for command in commands:
                await conn.execute(command)

async def connect():
    try:
        conn = await asyncpg.connect(user=user, password=password, 
                                     database=database, host=host)
        return conn

    except asyncpg.exceptions.PostgresError as db_error:
        print("Database error:", db_error)
    except ConnectionError as conn_error:
        print("Connection error:", conn_error)
    except Exception as error:
        print("Unexpected error:", error)

    return None

proxies = FreeProxy().get_proxy_list(1)

with open('chrome_version.txt', 'r') as my_file:
    
        versions = deque([version.strip() for version in my_file])

proxies = deque([proxy for proxy in proxies])
        
def get_proxy_response(url):
    
    MAX_V_ATTEMPTS = 5

    p_attempt = 0
    while p_attempt < len(proxies) + 1:

        proxy = proxies[0]
        p_attempt += 1

        v_attempt = 0 
        while v_attempt < MAX_V_ATTEMPTS + 1:

            version = versions[0]
            v_attempt += 1

            # headers = {
            #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            #     'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36 Edg/125.0.0.0'
            #     }

            headers = {
                'User-Agent': f'Chrome/{version}'
            }

            response = requests.get(url=url, proxies={'http': proxy}, headers=headers)

            if response.status_code == 200:
            
                return response 

            # elif response.status_code == 503:             
            #     print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tm:get_proxy_response\tproxy:{proxy}\tattempt:{v_attempt}\tversion:{version}\tstatus:{response.status_code}.')
            # else:
            #     print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tm:get_proxy_response\tUnhandled error status code:{response.status_code}')
            
            versions.append(versions.popleft())

        #print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tm:get_proxy_response\tProxy {proxy} compromised\tProxy attempt:{p_attempt}')
        proxies.append(proxies.popleft())

    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tm:get_proxy_response\tFailed retrieve response from url:{url}')





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

    def cheсk_pagination(self):

        if len(self.subgroups) > 0:
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

    def cheсk_endpoints(self):

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
                hash_64 = int.from_bytes(hash_object, byteorder='little', signed=True)
                price = card_product_bottom[i].find('.//div[@class="price"]/p[@class="price_new"]').text

                if hash_64 not in hashes:

                    hashes.append(hash_64)
                    product = Product(name)
                    product.link = link
                    product.price = Decimal(price.replace(' ', '').replace('грн.', ''))
                    product.hash = hash_64
                    #print(f'name: {product.name}, price: {product.price}')
                    self.products.append(product)
   
    async def insert_data(self):

        # conn = await asyncpg.connect(user=user, password=password, database=database, host=host)
        conn = await connect()
        values_products = []
        values_dates = []
        time = datetime.now()

        for prod in self.products:
            values_products.append((prod.hash, prod.name, prod.link))
            values_dates.append((time, prod.hash, prod.price))
            unique_values_dates = list({(v[0], v[1]): v for v in values_dates}.values())
        # await conn.execute("""ALTER TABLE products_data 
        #                   ADD CONSTRAINT example_table_pk PRIMARY KEY (time, product_id);""")
      
#         await conn.execute("""
#     DROP TABLE products_data;
#     DROP TABLE products;
# """)

        await conn.executemany("INSERT INTO products(product_id, name, link) VALUES($1,$2,$3) ON CONFLICT (product_id) DO NOTHING;", values_products)
        await conn.executemany("INSERT INTO products_data(data_id, time, product_id, price) VALUES(DEFAULT,$1,$2,$3)", unique_values_dates )
        
        # for command in commands:
        #     await conn.execute(command)
        # sql = await conn.fetch("SELECT * FROM products WHERE product_id = 412351078026438094;")
        # for i in sql:
        #     print(i)
        await conn.close()


    async def get_all_products(self):

        if len(self.subgroups)>0:
            for sub in self.subgroups:
                await sub.get_all_products()

        if len(self.products)>0:

            name = []
            price = []
            link = []

            for prod in self.products:
                name.append(prod.name)
                price.append(prod.price)
                link.append(prod.link)

                #values = [(prod.name, prod.price, prod.link)]
                #print(values)
            #config = load_config()

            try:
                
                if not asyncio.get_event_loop().is_running():
                    asyncio.run(self.insert_data())
                else:
                    await self.insert_data()

            except Exception as error:
                print("another error:", error)

            dt = datetime.now() 
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

            with pd.ExcelWriter('path_to_file.xlsx') as writer:
                table.to_excel(writer)
    

           
    async def refresh_products(self):
        conn = await connect()
    
        try:
            sql = await conn.fetch("SELECT product_id FROM products")
            db_hashes = {item['product_id'] for item in sql}

            values_products = []
            values_dates = []
            time = datetime.now()
            
            for prod in self.products:
                if prod.hash not in db_hashes:
                    print(f'{prod.name} doesn`t match')
                    values_products.append((prod.hash, prod.name, prod.link))
                    values_dates.append((time, prod.hash, prod.price))
            
            if values_products:
                await conn.executemany(
            """
            INSERT INTO products (product_id, name, link) VALUES ($1, $2, $3) ON CONFLICT (product_id) DO NOTHING""", values_products)
    
            if values_dates:
                await conn.executemany(
            """INSERT INTO products_data (time, product_id, price) VALUES ($1, $2, $3)""", values_dates)

        except asyncpg.exceptions.PostgresError as db_error:
            print("Error updating product:", db_error)

        finally:
            if conn:
                await conn.close()
            


    async def update_products(self):
        conn = await connect()
        try:
            sql = await conn.fetch("SELECT product_id, price FROM products_data")
            
            db_prices = {item['product_id']: item['price'] for item in sql}

            for prod in self.products:
                if prod.hash in db_prices:
                    db_price = db_prices[prod.hash]
                    if prod.price != db_price:
                        print(f"Price mismatch for {prod.name}: {prod.price} != {db_price}")
                        await conn.execute(""" UPDATE products_data SET price = $1 WHERE product_id = $2 """, prod.price, prod.hash)
                else:
                    print(f"{prod.name} not found in the database")
                    await self.refresh_products()

        except asyncpg.exceptions.PostgresError as db_error:
            print("Error fetching products data:", db_error)
                
        finally:
            if conn:
                await conn.close()



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

    pool = await get_db_pool() 
    await execute_db_commands(commands, pool)
    await pool.close()

    if os.path.exists('old_responses.pkl'):
        with open('old_responses.pkl', 'rb') as my_file:
            try:
                load_data = pickle.load(my_file)
                root = load_data
            except pickle.UnpicklingError:
                print('error')
    else:

        for i in range(120):
            response = get_proxy_response(url)
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

        root.cheсk_endpoints()

        root.cheсk_pagination()

        await root.get_responses()

        with open('old_responses.pkl', 'wb') as my_file:
            pickle.dump(root, my_file)


    root.href_all_products() 
    await root.get_all_products()
    await root.refresh_products()
    await root.update_products()
    

    print(f'Время прошло{time.monotonic() - start_time}')



if __name__ == '__main__': 
    asyncio.run(main())

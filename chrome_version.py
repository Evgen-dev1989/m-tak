import sqlite3
from requests import Session
import requests
from lxml import html
from lxml.html import HtmlElement
import time
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

#url = 'https://www.cvedetails.com/version-list/1224/15031/1/Google-Chrome.html?sha=77c8b67f5f2ab2ef1626d2990521d8f55926f9eb&order=1&trc=9258'

headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0'
            }

# response = requests.get(url=url, headers=headers)
# start_element: HtmlElement = html.fromstring(response.text)


class Chrome(object):
    def __init__(self):
        self.response = None
        self.url = None
        self.links = []
        self.address = []

    async def get_responses(self):
            
            tasks = []
            for endpoint in self.links:
                task = asyncio.create_task(endpoint.get_response())
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def get_response(self):
            loop = asyncio.get_event_loop()
            self.response = await loop.run_in_executor(None, partial(requests.get, url=self.url, headers=headers))
           

    def check_pagination(self):
        self.response = requests.get(url=self.url, headers=headers)
        print(self.response.status_code)
        pagin: HtmlElement = html.fromstring(self.response.text).xpath('.//div[@class="paging"]/a')
        for i in pagin:
            link_pagin = Chrome()
            link_pagin.url = "https://www.cvedetails.com/" + i.get('href')
            self.links.append(link_pagin)
            #print(link_pagin)

    def get_address(self):
        for link in self.links:
            #response = requests.get(url=link, headers=headers)
            element: HtmlElement = html.fromstring(self.response.text).find('.//div[@class="table-responsive"]//tbody') 
            element = element.xpath('./tr')
            write_list = []
            for i in element:
                link = str.strip(i.find('./td').text) 
                
                write_list.append(link)
                self.address.append(link)
            with open('chrome_versions.txt', 'w+') as f:
                for items in write_list:
                    f.write('%s\n' %items)
                f.close()   
       



async def main():
    start_time = time.monotonic()

    root = Chrome()
    root.url = 'https://www.cvedetails.com/version-list/1224/15031/1/Google-Chrome.html?sha=77c8b67f5f2ab2ef1626d2990521d8f55926f9eb&order=1&trc=9258'

    root.check_pagination()
    await root.get_responses()
    root.get_address()


    print(f'Время прошло{time.monotonic() - start_time}')
if __name__ == '__main__': 
    asyncio.run(main())

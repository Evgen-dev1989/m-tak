import requests
from fp.fp import FreeProxy
from lxml import html
from lxml.html import HtmlElement
import os
import random

def get_proxy_response(url, proxies):

    user_agenr = ['Edg/123.0.0.0', 'AppleWebKit/537.36(KHTML, like Gecko)', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Chrome/123.0.0.0 Safari/537.36']

    headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': random.choice(user_agenr)
            }
    for proxy in proxies:

        response = requests.get(url=url,proxies={'http': proxy}, headers=headers)
        print(response.status_code, proxy) 

        if response.status_code == 200:
            break
    return response



proxies = FreeProxy(country_id=['US', 'BR'], timeout=0.3, rand=True).get_proxy_list(1)
#print(proxy)
# proxies = {
#     'http': proxy
 
# }

url = 'https://militarist.ua/ua/'



for i in range(1000):
    
    response = get_proxy_response(url, proxies)


        



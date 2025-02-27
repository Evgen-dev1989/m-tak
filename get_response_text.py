import requests
from lxml import html
from lxml.html import HtmlElement

url = 'https://militarist.ua/ua/catalog/weapon-accessories/tyuning-oruzhiya/soshki/'
headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0'
    }


def save_To_html(file_name, data):
       
       with open(file_name, 'a', encoding="utf-8") as my_file:
                my_file.write(data)

response = requests.get(url=url, headers=headers)

save_To_html('get_response_text.html', response.text)
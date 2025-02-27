Web Scraping via Proxy with Chrome Headers

Overview
This project collects information from websites using a proxy server while dynamically modifying the headers to mimic different Chrome versions. Each product is uniquely identified using a hash of its URL.

Features
- Uses proxy servers to bypass restrictions.
- Modifies request headers to simulate different Chrome browser versions.
- Extracts and processes web data using `requests` and `lxml`.
- Implements MurmurHash to generate unique keys for each product.
- Stores and manages data using SQLite and Pandas.
- Supports asynchronous operations with `asyncio` and `asyncpg`.

Dependencies
Ensure you have the following Python libraries installed:
bash
pip install requests fp lxml configparser sqlite3 asyncio pandas asyncpg mmh3 struct json


Installation
Clone this repository and navigate to the project directory:
bash
git clone https://github.com/Evgen-dev1989/m-tak.git
cd yourrepository

Usage
1. Retrieve a free proxy using `fp.fp.FreeProxy`.
2. Modify headers dynamically to include different Chrome versions.
3. Parse HTML responses using `lxml`.
4. Store data efficiently in a database.

Example
`python
from fp.fp import FreeProxy
import requests
from lxml import html

proxy = FreeProxy().get()
session = requests.Session()
session.proxies = {"http": proxy, "https": proxy}
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0.0.0 Safari/537.36"}
response = session.get("https://example.com", headers=headers)
parsed_html = html.fromstring(response.content)

License
This project is licensed under the MIT License.

Contact
For any inquiries, please contact [camkaenota@gmail.com].


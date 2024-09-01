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



conn = psycopg2.connect(**load_config())


commands = (
   
            """
        CREATE TABLE IF NOT EXISTS products(
            product_id SERIAL PRIMARY KEY,
            name VARCHAR(250) NOT NULL,
            link VARCHAR(250) NOT NULL
            )
        """,
         """
        CREATE TABLE IF NOT EXISTS products_data (
                transaction SERIAL PRIMARY KEY,
                price INTEGER NOT NULL,
                time timestamp NOT NULL,
                FOREIGN KEY (product_id)
                REFERENCES products (product_id)
                ON UPDATE CASCADE ON DELETE CASCADE
                )
        """,
         """
            DROP TABLE IF EXISTS products CASCADE;
        """,
            """
            DROP TABLE IF EXISTS products_data CASCADE;
        """,
)

try:
    config = load_config()
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
except (psycopg2.DatabaseError, Exception) as error:
    print(error)

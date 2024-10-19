import asyncpg
import asyncio

async def main():

    conn = await asyncpg.connect(user='postgres', password='1111',database='m-tak', host='localhost')

    a = await conn.fetch('SELECT * FROM products WHERE product_id = 4189599120588895377;')
    print(a)


if __name__ == '__main__': 
    asyncio.run(main())

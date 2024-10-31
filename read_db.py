import asyncpg
import asyncio

async def main():

    conn = await asyncpg.connect(user='postgres', password='1111',database='m-tak', host='localhost')

    a = await conn.fetch("SELECT product_id, time FROM products_data ORDER BY product_id ASC;")
    with open("output.txt", "w", encoding="utf-8") as file:
        file.write(str(a))


if __name__ == '__main__': 
    asyncio.run(main())

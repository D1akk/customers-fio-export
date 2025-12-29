import asyncio
import psycopg2
import aiohttp
from openpyxl import Workbook

DB = dict(
    host="127.0.0.1",
    port=5432,
    dbname="",
    user="postgres",
    password="postgres",
)

API_URL = ""
BATCH = 1000
CONCURRENCY = 20

def cus_ids():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor(name="c")
    cur.execute("SELECT DISTINCT cus_id FROM transactions")
    while rows := cur.fetchmany(BATCH):
        for (cid,) in rows:
            yield str(cid)
    cur.close()
    conn.close()


async def fio(session, sem, cid):
    async with sem:
        async with session.get(API_URL, params={"cusId": cid}) as r:
            return await r.text() if r.status == 200 else None


async def main():
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("customers")
    ws.append(["cus_id", "fio"])

    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        batch = []
        for cid in cus_ids():
            batch.append(cid)
            if len(batch) == BATCH:
                res = await asyncio.gather(
                    *[fio(session, sem, c) for c in batch]
                )
                for c, f in zip(batch, res):
                    ws.append([c, f])
                batch.clear()

        if batch:
            res = await asyncio.gather(
                *[fio(session, sem, c) for c in batch]
            )
            for c, f in zip(batch, res):
                ws.append([c, f])

    wb.save("customers.xlsx")


if __name__ == "__main__":
    asyncio.run(main())

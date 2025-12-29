import asyncpg
from db_config import DB_CONFIG

pool: asyncpg.Pool | None = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(
        **DB_CONFIG,
        min_size=5,
        max_size=20
    )

async def close_db():
    await pool.close()

GET_CUS_FIO_BY_ID = """
    SELECT cus_id
    FROM public.transactions
    WHERE cus_id = $1
"""

# cus_id пока у меня на тестовом строковый
async def get_cus_fio_by_id(cus_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(GET_CUS_FIO_BY_ID, cus_id)
        if row:
            return dict(row)
        return None

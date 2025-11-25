from typing import Optional

import asyncpg

from config import DB_CONFIG

_pool: Optional[asyncpg.pool.Pool] = None

async def init_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["dbname"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            min_size=1,
            max_size=10,
        )
    return _pool

def get_db_pool() -> asyncpg.pool.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call init_db_pool() at app startup.")
    return _pool

# helper to run simple query
async def fetch(query: str, *args):
    pool = get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def fetchrow(query: str, *args):
    pool = get_db_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)

async def execute(query: str, *args):
    pool = get_db_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)

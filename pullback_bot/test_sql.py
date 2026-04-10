import aiosqlite
import asyncio

async def test():
    db = await aiosqlite.connect('/Users/abid/Projects/pullback/pullback_bot/trades.db')
    c = await db.execute('SELECT count(*) FROM scanner_log')
    print("COUNT:", await c.fetchone())
    await db.close()

asyncio.run(test())

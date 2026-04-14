import asyncio
import ccxt.async_support as ccxt
import os

async def test():
    exchange = ccxt.binance({
        "apiKey": "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia",
        "secret": """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----""",
        "options": {"defaultType": "future", "adjustForTimeDifference": True}
    })
    
    try:
        # ccxt handles Ed25519 internally if provided properly, wait ccxt might not support Ed25519 out of the box easily. Let's see.
        order = await exchange.create_order(
            symbol="TRX/USDT:USDT",
            type="stop_market",
            side="sell",
            amount=1, # Doesn't matter, we want to see the endpoint
            params={
                "stopPrice": 0.3000,
                "positionSide": "LONG"
            }
        )
        print("Success:", order)
    except Exception as e:
        print("Failed:", e)
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test())

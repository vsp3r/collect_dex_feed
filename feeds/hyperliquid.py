import websockets
import asyncio
import json


class HyperliquidConnector:
    def __init__(self, queue, symbols):
        self.queue = queue
        self.symbols = symbols

    def start(self):
        asyncio.run(self.run())

    async def run(self):
        print('starting HL run')
        await self
    # async def connect(self):
    #     uri = "wss://api.hyperliquid.xyz/ws"
    #     async with websockets.connect(uri) as websocket:
    #         for coin in self.symbols:
    #             subscription_message = {
    #                 "method": "subscribe",
    #                 "subscription": {"type": "l2Book", "coin":coin}
    #             }

    #             await websocket.send(json.dumps(subscription_message))
    #             # print(f'sent sub')
    #             # msg = await websocket.recv()
    #             # msg2 = await websocket.recv()
    #         raw_message = await websocket.recv()
    #         print(f'received buffers {msg} + {msg2}')
    #         n = 0
    #         while True:
    #             message = await websocket.recv()
    #             n += 1
    #             # print(f'HYPERLIQUID({n}): {message}')
    #             self.callbacks[0](json.loads(message), n)
        
    
    # async def run(self):
    #     await self.connect()
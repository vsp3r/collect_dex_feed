import websockets
import asyncio
import json


class BinanceConnector:
    def __init__(self, queue, symbols):
        self.queue = queue
        self.symbols = symbols

        self.subscription_id_counter = 0
        self.queued_subscriptions = []
        self.active_subscriptions = []
        self.ws_url = 'wss://fstream.binance.com/ws'
        print(f'init binance w/ {symbols}')
    
    
    # def start(self):
    #     asyncio.run(self.run())

    async def run(self):
        print('starting run')
        # await asyncio.gather(
        #     self.connect_feed()
        # )
        await self.connect_feed()

    async def connect_feed(self):
        print('start connect')
        async with websockets.connect(self.ws_url) as ws:
            print('start ws')
            for coin in self.symbols:
                coin = coin.lower() + 'usdt'
                await self.subscribe(ws, coin)
            
            while True:
                message = await ws.recv()
                await self.process_data(message)


    async def subscribe(self, ws, coin):

        subscription_msg = {
            "method":"SUBSCRIBE",
            "params":[
                coin+"@depth",
                # coin+"@aggTrade"
            ],
            "id":1
        }
        print('sending sub')
        await ws.send(json.dumps(subscription_msg))

    async def process_data(self, message):
        print(message)
        self.queue.put_nowait(('binance', 'test', message))

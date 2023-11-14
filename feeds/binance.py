import websockets
import asyncio
import json
from queue import Full


class BinanceConnector:
    def __init__(self, queue, size_counter, symbols):
        self.queue = queue
        self.size_counter = size_counter
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
            await asyncio.gather(*(self.subscribe(ws, coin.lower() + 'usdt')
                                  for coin in self.symbols))
            
            while True:
                message = await ws.recv()
                self.process_data(message)


    async def subscribe(self, ws, coin):

        subscription_msg = {
            "method":"SUBSCRIBE",
            "params":[
                coin+"@depth@0ms",
                coin+"@aggTrade",
                coin+"@markPrice@1s",
                coin+"bookTicker"
            ],
            "id":1
        }
        print('sending sub')
        await ws.send(json.dumps(subscription_msg))
        _ = await ws.recv() # drop first message

    def process_data(self, message):
        # print(message[:100])
        try:
            coin = json.loads(message)['s']
            self.queue.put_nowait(('binance', coin, message))
            with self.size_counter.get_lock():
                self.size_counter.value += 1
        except Full:
            print('QUEUE FULL, DROPPING ITEM')

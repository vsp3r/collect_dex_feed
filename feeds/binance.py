import websockets
import asyncio
import json
import requests
from queue import Full


class BinanceConnector:
    def __init__(self, queue, size_counter, symbols):
        self.queue = queue
        self.size_counter = size_counter
        self.symbols = symbols
        

        self.subscription_id_counter = 0
        self.queued_subscriptions = []
        self.active_subscriptions = []

        self.api_url = "https://fapi.binance.com"
        self.ws_url = 'wss://fstream.binance.com/ws'
    
    async def connect(self):
        print('start binance connect')
        await self.approve_symbols()
        async with websockets.connect(self.ws_url) as ws:
            # print('start binance ws')
            symbol_chunks = [self.symbols[i:i + 10] for i in range(0, len(self.symbols), 10)]
            for chunk in symbol_chunks:
                await asyncio.gather(*(self.subscribe(ws, coin.lower() + 'usdt')
                                    for coin in chunk))
                
                # Binance only accepts 10 incoming mesages per sec. So buffer a little
                await asyncio.sleep(1.2)
            print('subbed all binance feeds, processing now')
            while True:
                message = await ws.recv()
                asyncio.create_task(self.process_data(message))

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
        # print('sending sub')
        await ws.send(json.dumps(subscription_msg))
        # _ = await ws.recv() # drop first message

    async def process_data(self, message):
        data = json.loads(message)
        try:
            if 'e' in data:
                coin = data['s']
                self.queue.put_nowait(('binance', coin, message))
                with self.size_counter.get_lock():
                    self.size_counter.value += 1
        except KeyError as ke:
            pass
        except Full:
            print('QUEUE FULL, DROPPING ITEM')


    async def approve_symbols(self):
        info_url = self.api_url + '/fapi/v1/exchangeInfo'
        response = requests.get(info_url)
        data = response.json()

        symbols = [item['symbol'][:-4] for item in data['symbols'] if 'USDT' in item['symbol']]
        self.symbols = [coin for coin in self.symbols if coin in symbols]
        print(f'init binance w/ {self.symbols} total length" {len(self.symbols)}')

        
    async def close(self):
        print('binance closing')
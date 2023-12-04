import websockets
import asyncio
import json
from queue import Full


class HyperliquidConnector:
    def __init__(self, queue, size_counter, symbols):
        self.queue = queue
        self.size_counter = size_counter
        self.symbols = symbols

        self.ws_url = 'wss://api.hyperliquid.xyz/ws'
        print(f'init hyperliquid w/ {symbols}')

    async def connect(self):
        print('start hl connect')
        async with websockets.connect(self.ws_url) as ws:
            print('start hl websocket')
            await asyncio.gather(*(self.subscribe(ws, coin)
                                  for coin in self.symbols))

            print('hyperliquid finished subbing, parsing messages now')
            while True:
                message = await ws.recv()
                asyncio.create_task(self.process_data(message))
        
    async def subscribe(self, ws, coin):
        subscription_message = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin":coin}
        }
        # print(f'sending hl sub {coin}')
        await ws.send(json.dumps(subscription_message))
        subscription_message = {
            "method": "subscribe",
            "subscription": {"type": "trades", "coin":coin}
        }
        # print(f'sending hl sub {coin}')
        await ws.send(json.dumps(subscription_message))

    async def process_data(self, message):
        data = json.loads(message)
        # print(message)
        try:
            if data['channel'] == 'l2Book':
                coin = data['data']['coin']
                self.queue.put_nowait(('hyperliquid', coin, message))
                with self.size_counter.get_lock():
                    self.size_counter.value += 1
            if data['channel'] == 'trades':
                # print(data)
                for trade in data['data']:
                    # print(trade)
                    self.queue.put_nowait(('hyperliquid', trade['coin'], str(trade)))
                    with self.size_counter.get_lock():
                        self.size_counter.value += 1

        except KeyError as ke:
            pass
        except Full:
            print('QUEUE FULL, DROPPING ITEM')

    async def close(self):
        print('Closing hl thing')
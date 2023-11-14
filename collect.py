import json
from src.utils import config_parse
from multiprocessing import Queue, Pool, Process
import asyncio
import os
import signal
import datetime
import time
from dotenv import load_dotenv
from src.data_writer import DataWriter

from feeds.binance import BinanceConnector
# from feeds.hyperliquid import HyperliquidConnector

# every 5 min send discord message that we're good



# def writer_proc(queue, output):
#     # time.sleep(3)
#     while True:
#         data = queue.get()
#         if data is None:
#             break
#         symbol, timestamp, message = data
#         date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d')
#         with open(os.path.join(output, '%s_%s.dat' % (symbol, date)), 'a') as f:
#             f.write(str(int(timestamp * 1000000)))
#             f.write(' ')
#             f.write(message)
#             f.write('\n')



def start_writer(queue):
    writer = DataWriter(queue)
    writer.run()


async def main(queue, symbols, writer):
    print('entered main')

    # with Pool(processes=3) as pool:
    print('started pool')
    binance = BinanceConnector(queue, symbols)
    streams['binance'] = binance

    writer_p = Process(target=start_writer, args=(queue,))
    writer_p.start()

    await asyncio.gather(
        *(stream.run() for stream in streams.values()),
    )

    # pool.apply(binance.start)
    print('ran run')

        # hyperliquid = HyperliquidConnector(queue, symbols)
        # pool.apply_async(hyperliquid.run)
        # streams['hyperliquid'] = hyperliquid

        # pool.apply_async(writer.run)
        # pool.close()
        # pool.join()



async def shutdown(loop, streams, writer, queue):
    print("Shutting down")

    # Close streams
    for stream_name, stream in streams.items():
        await stream.close()

    # Close writer
    await writer.close()

    queue.close()
    queue.join_thread()

    loop.stop()




if __name__ == '__main__':
    queue = Queue()
    # queue = 'x'
    config = config_parse('config.json')
    symbols = config['Symbols']
    streams = {}

    loop = asyncio.get_event_loop()
    writer = DataWriter(queue)

    # for sig in [signal.SIGTERM, signal.SIGINT]:
    #     loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(loop, streams, writer, queue)))

    print("starting loop")
    loop.run_until_complete(main(queue, symbols, writer))
    # asyncio.run(main(queue, symbols))
    # main(queue, symbols)
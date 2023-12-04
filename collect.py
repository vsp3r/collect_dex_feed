import json
from src.utils import config_parse
from src.discord_webhook import post_alert
from multiprocessing import Queue, Pool, Process, Value
import asyncio
import os
import signal
from datetime import datetime
import time
from dotenv import load_dotenv
from src.data_writer import DataWriter

from feeds.binance import BinanceConnector
from feeds.hyperliquid import HyperliquidConnector


def start_writer(queue, size_counter):
    writer = DataWriter(queue, size_counter)
    try:
        writer.run()
    except KeyboardInterrupt:
        print('keyboard detected writer')
        writer.close()

def monitor_queue(size_counter):
    while True:
        try:
            current_time = datetime.now()
            post_alert(os.getenv("DISCORD_WEBHOOK_URL"), f"({current_time}) still running w/ queue: {size_counter.value}")
            print(f'Queue size: {size_counter.value}')
            time.sleep(4)
        except KeyboardInterrupt:
            print("keyboard detected monitor queue")
            break


# def process_handler(target_class, *args):
#     try:
#         obj = target_class(*args)
#         obj.run()
#     except KeyboardInterrupt:
#         print(f'Keyboard interrupt detecting in p_handler of {obj.__name__}')
#         obj.close()


async def main(loop, queue, symbols, size_counter):
    processes = []
    streams = {}

    binance = BinanceConnector(queue, size_counter, symbols)
    streams['binance'] = binance

    hyperliquid = HyperliquidConnector(queue, size_counter, symbols)
    streams['hyperliquid'] = hyperliquid

    writer_p = Process(target=start_writer, args=(queue,size_counter,))
    writer_p.start()
    processes.append(writer_p)

    monitor_p = Process(target=monitor_queue, args=(size_counter,))
    monitor_p.start()
    processes.append(monitor_p)


    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(loop, streams, processes, queue)))

    await asyncio.gather(
        *(stream.connect() for stream in streams.values()),
    )


async def shutdown(loop, streams, processes, queue):
    print("SHUTDOWN METHOD CALLED")

    # Close streams
    for stream_name, stream_obj in streams.items():
        await stream_obj.close()

    # Close writer
    queue.put("SHUTDOWN")

    for p in processes:
        print("joining processes")
        p.join()

    queue.close()
    queue.join_thread()

    loop.stop()


if __name__ == '__main__':
    load_dotenv()
    queue = Queue()
    size_counter = Value('i', 0)
    # queue = 'x'
    config = config_parse('config.json')
    symbols = config['Symbols']

    print(len(symbols))
    streams = {}

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main(loop, queue, symbols, size_counter))
    except Exception as e:
        post_alert(os.getenv("DISCORD_WEBHOOK_URL"), f"({time.time()}) we have an issue {e}")

    # asyncio.run(main(queue, symbols))
    # main(queue, symbols)


# 2332 messages over 4.6416 seconds

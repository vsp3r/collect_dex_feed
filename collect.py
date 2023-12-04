import json
import logging
from multiprocessing import Queue, Process
import asyncio
import os
import signal
from datetime import datetime
from dotenv import load_dotenv

from src import Application, DataWriter, post_alert, config_parse, config_validator

import feeds


def process_handler(target_class, *args):
    try:
        obj = target_class(*args)
        obj.run()
    except KeyboardInterrupt:
        logging.debug(f'Keyboard INT detected in {target_class}')
        obj.close()


async def shutdown(processes):
    print("SHUTDOWN METHOD CALLED")

    for exchange, process_tuple in processes.items():
        process_tuple[0].put("SHUTDOWN")
        process_tuple[0].close()
        process_tuple[0].join_thread()

        process_tuple[1].close()
        process_tuple[1].join()
        process_tuple[2].close()
        process_tuple[2].join()

    loop.stop()

    # # Close streams
    # for stream_name, stream_obj in streams.items():
    #     await stream_obj.close()

    # # Close writer
    # queue.put("SHUTDOWN")

    # for p in processes:
    #     print("joining processes")
    #     p.join()

    # queue.close()
    # queue.join_thread()


async def main():
    config = config_parse('config.yaml')
    if not config_validator(config):
        raise Exception("configuration failed validation")
    
    symbols = config['Symbols']
    exchanges = [exch[0].upper() + exch[1:].lower() for exch in config['Exchanges']]
    processes = {}

    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, shutdown(processes))

    for exchange in exchanges:
        queue = Queue()
        writer_p = Process(target=process_handler, args=(DataWriter, queue)).start()
        connector_p = Process(target=process_handler, args=(getattr(feeds, f'{exchange}Connector'), queue, symbols)).start()
        # processes.extend([writer_p, connector_p])
        processes[exchange] = (queue, writer_p, connector_p)

    
    for p in processes:
        p.join()

        # writer_p = Application(f'{exchange}_writer', DataWriter, queue)
        # connector = Application(f'{exchange}_connector', getattr(feeds, f'{exchange}Connector'), queue, symbols)
        # streams[exchange] = (connector, writer_p)


    # binance = BinanceConnector(queue, size_counter, symbols)
    # streams['binance'] = binance

    # hyperliquid = HyperliquidConnector(queue, size_counter, symbols)
    # streams['hyperliquid'] = hyperliquid

    # writer_p = Process(target=start_writer, args=(queue,size_counter,))
    # writer_p.start()
    # processes.append(writer_p)

    # monitor_p = Process(target=monitor_queue, args=(size_counter,))
    # monitor_p.start()
    # processes.append(monitor_p)

    # await asyncio.gather(
    #     *(stream.connect() for stream in streams.values()),
    # )

if __name__ == '__main__':
    load_dotenv()
    logging.getLogger("COLLECT")
    logging.basicConfig(filename=f'collect.log', format="%(asctime)s [%(levelname)-7s] [%(name)s] %(message)s",
                            level=logging.INFO)

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main())
    except Exception as e:
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        post_alert(os.getenv("DISCORD_WEBHOOK_URL"), f"({current_timestamp}) we have an issue {e}")
        logging.error(f'Collect level exception {e}')


# def monitor_queue(size_counter):
#     while True:
#         try:
#             current_time = datetime.now()
#             post_alert(os.getenv("DISCORD_WEBHOOK_URL"), f"({current_time}) still running w/ queue: {size_counter.value}")
#             print(f'Queue size: {size_counter.value}')
#             time.sleep(4)
#         except KeyboardInterrupt:
#             print("keyboard detected monitor queue")
#             break

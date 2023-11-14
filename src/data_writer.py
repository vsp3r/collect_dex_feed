import os
import time
import asyncio

class DataWriter:
    def __init__(self, queue, size_counter, base_dir='data'):
        self.queue = queue
        self.size_counter = size_counter
        self.base_dir = base_dir
        self.running = True

    def run(self):
        print('starting data_writer run')
        while self.running or not self.queue.empty():
            # try:
            message = self.queue.get()

            with self.size_counter.get_lock():
                self.size_counter.value -= 1


            if message is None:
                break
            exchange, symbol, data = message
            self.write_data(exchange, symbol, data)
            # except Exception as e:
            #     print(f'Error in writer: {e}')
            #     break
            
    def write_data(self, exchange, symbol, data):
        dir_path = os.path.join(self.base_dir, exchange, symbol)
        os.makedirs(dir_path, exist_ok=True)

        filename = time.strftime("%Y_%m_%d") + ".txt"
        file_path = os.path.join(dir_path, filename)

        with open(file_path, 'a') as f:
            f.write(str(int(time.time() * 10000000)))
            f.write(' ')
            # f.write(self.queue.qsize())
            # f.write(' ')
            f.write(data)
            f.write('\n')
        
    def close(self):
        # Don't process new data
        self.running = False

        # Empty the queue
        while not self.queue.empty():
            asyncio.sleep(1)

        print("Datawriter closed gracefully")
        

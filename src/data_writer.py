import os
import time
import asyncio

class DataWriter:
    def __init__(self, queue, base_dir='data'):
        self.queue = queue
        self.base_dir = base_dir
        self.running = True
        self.msg_num = 0
        self.start_time = time.perf_counter_ns()

    def run(self):
        while self.running or not self.queue.empty():
            # try:
            message = self.queue.get()
            if message == "SHUTDOWN" or None:
                print('G0T SHUTDOWN MESSAGE')
                self.close()
                break

            self.parse_msg(message)


    def parse_msg(self, message):
        self.msg_num += 1

        exchange, symbol, data = message
        self.write_data(exchange, symbol, data)

    def write_data(self, exchange, symbol, data):
        dir_path = os.path.join(self.base_dir, exchange, symbol)
        os.makedirs(dir_path, exist_ok=True)

        filename = time.strftime("%Y_%m_%d") + ".txt"
        file_path = os.path.join(dir_path, filename)

        with open(file_path, 'a') as f:
            f.write(str(int(time.time() * 1_000_000_000)))
            f.write(' ')
            f.write(data)
            f.write('\n')
        
    def close(self):
        # Don't process new data
        self.running = False
        self.end_time = time.perf_counter_ns()

        # Empty the queue
        while not self.queue.empty():
            message = self.queue.get()
            self.parse_msg(message)

        print("Datawriter closed gracefully")
        print(f'{self.msg_num} messages processed over {(self.end_time - self.start_time)/1_000_000_000:.4f} seconds')
        

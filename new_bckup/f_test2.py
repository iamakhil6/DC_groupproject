from socket import socket, AF_INET, SOCK_STREAM
from functools import partial
import threading
import multiprocessing
import select
import queue
import json
import random
import dill
import datetime
import math
import sys
import time


class Worker():
    def __init__(self):
        ### Metrics ###
        self.t1 = datetime.datetime.now()
        self.msg_count = 0
        #################
        self.exit_flag = False
        self.id = 'w2'
        self.system_ip = "131.151.243.65"
        self.process_port = 8000
        self.msg_port = 8001
        self.result_port = 8002
        self.workers_id = ['w1', 'w3']
        self.servers_list = {"w1": ["131.151.243.64", 7000, 7001, 7002],
                             "w3": ["131.151.243.66", 9000, 9001, 9002]}
        self.threshold = 5
        self.numbers = list(range(1500000, 2000000, 100000))
        self.processes = queue.Queue()
        self.processes.queue = queue.deque(self.numbers)
        self.init_count = threading.active_count()
        self.misc_count = 9
        self.load = 0
        self.local_queue = queue.Queue(maxsize=5)
        self.server_queue = queue.Queue()
        self.server_results = queue.Queue()

    def process_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.process_port))
        msg_server.listen(5)
        print('Process listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(2048)
                    if data:
                        self.process_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def msg_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.msg_port))
        msg_server.listen(5)
        print('Message listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(2048)
                    data = data.decode("utf-8")
                    if data:
                        self.msg_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def result_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.result_port))
        msg_server.listen(5)
        print('Message listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(2048)
                    data = data.decode("utf-8")
                    if data:
                        self.result_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def process_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        func = dill.loads(data)
        # func_result = str(func())
        self.server_queue.put((wid, func))
        print('Received process from:', wid)

    def msg_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        if data == 'SMYP':
            if self.load >= self.threshold:
                self.send_process(worker_id=wid, p=self.processes.get())

    def result_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        if "RESULT" in data:
            print(data)
            data = data.split(' ')[1]
            server_res_w2.put(int(data))
            print(f"Result from {wid}:", data)

    def send_process(self, worker_id, p):
        func_text = dill.dumps(partial(sum_primes, p))
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[1]))
                connected = True
            except:
                pass
        client.sendall(func_text)
        self.msg_count += 1
        client.close()

    def send_msg(self, worker_id, msg):
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[2]))
                connected = True
            except:
                pass
        client.sendall(bytes(msg, encoding="utf-8"))
        self.msg_count += 1
        client.close()

    def send_result(self, worker_id, msg):
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[3]))
                connected = True
            except:
                pass
        client.sendall(bytes(msg, encoding="utf-8"))
        self.msg_count += 1
        client.close()

    def result_sender(self):
        while True:
            if not self.server_results.qsize() == 0:
                wid, result = self.server_results.get()
                result = "RESULT" + ' ' + result
                self.send_result(worker_id=wid, msg=result)

    def load_tracker(self):
        while True:
            self.load = threading.active_count() - self.init_count - self.misc_count

    def status_tracker(self):
        while True:
            print('Load:', self.load, '\n')
            print('Local Results:', list(results_w2.queue), '\n')
            print('Server Results:', list(server_res_w2.queue), '\n')
            if len(list(results_w2.queue)) + len(list(server_res_w2.queue)) == len(self.numbers):
                self.exit_flag = True
                time_taken = datetime.datetime.now() - self.t1
                print(time_taken)
                print("Messages Sent: ", self.msg_count)
                sys.exit()
            time.sleep(1)

    def request_process(self):
        while True:
            if self.load < self.threshold:
                worker = random.choice(self.workers_id)
                self.send_msg(worker, 'SMYP')
                time.sleep(1)

    def local(self):
        while True:
            if not self.processes.qsize() == 0:
                self.local_queue.put(self.processes.get())
                thread = threading.Thread(target=sum_primes, args=(self.local_queue.get(),))
                thread.start()
                if threading.active_count() >= self.threshold + self.init_count + self.misc_count:
                    thread.join()

    def server(self):
        while True:
            if not self.server_queue.qsize() == 0:
                wid, func = self.server_queue.get()
                wrapped_func = self.func_decorator(wid, func)
                thread = threading.Thread(target=wrapped_func)
                thread.start()

    def func_decorator(self, wid, func):
        def inner():
            self.server_results.put((wid, str(func())))
        return inner

    def start(self):
        while True:
            if self.exit_flag is True:
                sys.exit()


results_w2 = queue.Queue()
server_res_w2 = queue.Queue()


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""

    def isprime(x):
        """"pre-condition: n is a nonnegative integer
        post-condition: return True if n is prime and False otherwise."""
        if x < 2:
            return False
        if x % 2 == 0:
            return x == 2  # return False
        k = 3
        while k * k <= x:
            if x % k == 0:
                return False
            k += 2
        return True

    try:
        results_w2.put(sum([x for x in range(2, n) if isprime(x)]))
    except:
        return sum([x for x in range(2, n) if isprime(x)])


if __name__ == "__main__":
    worker = Worker()
    p_listener = threading.Thread(target=worker.process_listener)
    msg_listener = threading.Thread(target=worker.msg_listener)
    result_listener = threading.Thread(target=worker.result_listener)
    load_tracker = threading.Thread(target=worker.load_tracker)
    status_tracker = threading.Thread(target=worker.status_tracker)
    local = threading.Thread(target=worker.local)
    server = threading.Thread(target=worker.server)
    request_process = threading.Thread(target=worker.request_process)
    result_sender = threading.Thread(target=worker.result_sender)
    p_listener.start()
    msg_listener.start()
    result_listener.start()
    local.start()
    load_tracker.start()
    server.start()
    request_process.start()
    result_sender.start()
    status_tracker.start()
    worker.start()
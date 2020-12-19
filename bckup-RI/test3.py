from socket import socket, AF_INET, SOCK_STREAM
from ppserver import _NetworkServer
import threading
import multiprocessing
import select
import json
import math
import sys
import time
import pp


class RI():
    def __init__(self):
        self.id = 'w3'
        self.workers_id = ['w1', 'w2']
        self.workers_status = {self.workers_id[0]: False, self.workers_id[1]: False}  # if finished or not
        self.system_ip = "131.151.89.14"
        self.process_port = 9000
        self.msg_port = 9001
        self.servers_list = {"w1": ["131.151.89.158", 7000, 7001],
                             "w2": ["131.151.90.156", 8000, 8001]}
        self.processes = [500000, 750000, 1000000, 1250000, 1500000,
                          2000000, 2500000, 3000000, 3500000, 4000000]
        self.threshold = 5
        self.local_results = []
        self.server_results = []
        self.load = {self.id: len(self.processes)}

    def process_listener(self):
        print('listening to processes...')
        process_server = _NetworkServer(interface=self.system_ip, port=self.process_port)
        process_server.listen()

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
                    data = s.recv(1024)
                    data = data.decode("utf-8")
                    if data:
                        self.msg_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def msg_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        if data == "SMYP":
            ppservers = self.servers_list[wid]
            self.send_process(ppservers, wid=wid)
        if data == "SYAP":
            self.load[self.id] += 1
        if data == "NPTS":
            self.workers_status[wid] = True

    def send_process(self, ppservers, wid=None):
        if len(self.processes) > 0:
            ppservers = ppservers[0] + ":" + str(ppservers[1])
            job_server = pp.Server(ncpus=0, ppservers=(ppservers,))
            p_to_send = self.processes[-1]
            job = job_server.submit(sum_primes, (p_to_send,), (), ("math",))
            self.send_msg(wid, "SYAP")  # SYAP -> Sent you a process
            self.server_results.append(job())
            print(self.server_results)
            self.processes.remove(p_to_send)
            print(job_server.print_stats())
        else:
            self.send_msg(wid, "NPTS")  # NPTS -> No Process To Send

    def send_msg(self, worker_id, msg):
        client = socket(AF_INET, SOCK_STREAM)
        selected_server = self.servers_list[worker_id]
        client.connect((selected_server[0], selected_server[2]))
        client.sendall(bytes(msg, encoding="utf-8"))
        client.close()

    def start(self):
        pool = multiprocessing.Pool(processes=self.threshold).map_async(sum_primes,
                                                                        self.processes[:self.threshold])
        while True:
            if pool.ready():
                self.local_results = pool.get()
                print("Local Results: ", self.local_results)
                self.processes = self.processes[self.threshold:]
                self.load[self.id] -= self.threshold
                break
        while True:
            for worker in self.workers_id:
                if self.workers_status[worker] is False and self.load[self.id] < self.threshold:
                    self.send_msg(worker, "SMYP")
            if not False in self.workers_status.values():
                print('All Processes are complete!\n')
                print('Terminating program...\n')
                print('Done!')
                break


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

    return sum([x for x in range(2, n) if isprime(x)])


if __name__ == "__main__":
    obj = RI()
    thread1 = threading.Thread(target=obj.process_listener)
    thread2 = threading.Thread(target=obj.msg_listener)
    thread1.start()
    thread2.start()
    obj.start()

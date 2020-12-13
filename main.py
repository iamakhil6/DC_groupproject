from socket import socket, AF_INET, SOCK_STREAM
import multiprocessing
import json
import math
import sys
import time
import pp


class ReceiverInitiated():
    def __init__(self):
        self.id = 'w1'
        self.workers_id = ['w2', 'w3']
        self.system_ip = "131.151.243.64"
        self.port = 7001
        self.servers_list = {"w2": ["131.151.243.67", 8000, 8001],
                             "w3": ["131.151.243.65", 9000, 9001]}
        self.processes = [1000, 1500, 2000, 2500, 3000]
        self.results = []
        self.threshold = 2
        self.load_table = {"w1": 12, "w2": 10, "w3": 5}

    def listener(self):
        server = socket(AF_INET, SOCK_STREAM)
        server.bind((self.system_ip, self.port))
        server.listen(5)
        while True:
            c, addr = server.accept()
            response = c.recv(2048)
            response = response.decode("utf-8")
            try:
                self.load_table = json.loads(response)
            except:
                self.received_msg = response.split('-')
                if self.received_msg[0] == 'SMYP':
                    self.smyp = pp.Server(ncpus=0, ppservers=(self.received_msg[1]+':'+
                                                              self.received_msg[2]),)
                    p_to_send = self.processes[-1]
                    self.update_load_table(self.received_msg[3], 1)
                    self.update_load_table(self.id, -1)
                    self.smyp1 = self.smyp.submit(sum_primes, (p_to_send,), (isprime,), ("math",))
                    if self.symp1.finshed:
                        self.update_load_table(self.received_msg[3], -1)
            c.close()

    # def receive_process(self):
    #     server = socket(AF_INET, SOCK_STREAM)
    #     server.bind((self.))
    def request_process(self, worker):
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        client.connect((list(self.servers_list.values())[0][0], 8001))
        client.sendall(f"SMYP-{self.system_ip}-{self.port}-{self.id}")
        client.close()

    def update_load_table(self, wid, value):
        self.load_table[wid] += value
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)  # non-blocking port
        connected = False
        while not connected:
            try:
                data = json.dumps(self.load_table)
                client.connect((list(self.servers_list.values())[0][0], 8001))
                connected = True
                client.sendall(bytes(data, encoding="utf-8"))
                client.close()
            except:
                pass

    def start(self):
        if len(self.processes) <= self.threshold:
            self.result = [sum_primes(p) for p in self.processes[:self.threshold]]
        else:
            if self.load_table[self.id] < self.threshold:
                self.higher_value_queue = argmax(self.load_table['w2'], self.load_table['w3'])
                if self.higher_value_queue >= self.threshold:
                    self.request_process(self.workers_id[self.higher_value_queue])

    def send_process(self):
        self.servers_list = ["131.151.243.67:8000", "131.151.243.64:8000"]
        self.ppservers = (self.servers_list[0],)
        self.job_server = pp.Server(ncpus=0, ppservers=self.ppservers)
        for p in self.processes[self.threshold:]:
            self.job1 = self.job_server.submit(sum_primes, (p,), (isprime,), ("math",))
            self.result = self.job1()
            print(f"Sum of primes below {p} is", self.result)
        self.job_server.print_stats()


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""
    return sum([x for x in range(2, n) if isprime(x)])


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


if __name__ == "__main__":
    obj = ReceiverInitiated()
    listen = multiprocessing.Process(target=obj.listener)
    listen.start()
    obj.start()
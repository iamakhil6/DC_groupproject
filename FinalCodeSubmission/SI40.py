from threading import Thread
from socket import socket, AF_INET, SOCK_STREAM
import time
from ppserver import _NetworkServer


class SI40:
    def __init__(self, funcPort, loadPort,resultPort, thisSystemIP, thisSystemPort, processCount):
        self.processCount = processCount
        self.loadPort = loadPort
        self.funcPort = funcPort
        self.resultPort = resultPort
        self.thisSystemIP = thisSystemIP
        self.thisSystemPort = thisSystemPort
        self.textFromClient = r''


    def ppListening(self, ):
        server = _NetworkServer(interface=self.thisSystemIP, port=self.thisSystemPort)
        server.listen()

    def funcSocketListening(self,):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.funcPort))
        s.listen(15)
        print('Listening to port ', self.funcPort)
        while True:
            con, addr = s.accept()
            data = con.recv(1024)
            self.processCount += 1
            print('Calculating prime of ', int.from_bytes(data.split(b'_')[-1], 'big'))
            p = Thread(target=self.mathTask, args=(int.from_bytes(data.split(b'_')[-1], 'big'), con.getpeername()[0], False, ))#self.mathTask)
            p.start()


    def loadSocketListening(self,):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.loadPort))
        s.listen(5)
        print('Listening to port ', self.loadPort)
        while True:
            con, addr = s.accept()
            data = con.recv(1024)
            if b'system load' in data:
                con.send(self.processCount.to_bytes(2, 'big'))

    def fibonacci(self, n):
        if n < 2:
            return n
        else:
            return self.fibonacci(n - 1) + self.fibonacci(n - 2)

    def sum_primes(self, n):
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

    def mathTask(self, senderProcessID, con=None, local=False):
        if local:
            print('yet to write')
            self.sum_primes(senderProcessID)
            self.processCount -= 1
            self.resultDict[senderProcessID] = True
        else:
            num = self.sum_primes(senderProcessID)
            self.processCount -= 1
            s = socket(AF_INET, SOCK_STREAM)
            s.connect((con, self.resultPort))
            s.sendall(b'Result for process ID_'+bytes(str(senderProcessID), 'utf-8')+b'_is_'+ bytes(str(num), 'utf-8'))
            s.close()

if __name__ == '__main__':
    obj = SI40(10040, 10060, resultPort=10070, thisSystemIP='131.151.243.67',thisSystemPort=10000, processCount=0)

    t1 = Thread(target=obj.funcSocketListening)
    t1.start()
    t2 = Thread(target=obj.loadSocketListening)
    t2.start()
    # t3 = Thread(target=obj.ppListening)
    # t3.start()

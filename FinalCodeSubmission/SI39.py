from socket import socket, AF_INET, SOCK_STREAM
import time, csv
from threading import Thread
import numpy as np
import pickle as pkl


class SI39:
    def __init__(self, funcPort, loadPort, resultPort, processCount):
        self.processCount = processCount
        self.loadPort = loadPort
        self.funcPort = funcPort
        self.resultPort = resultPort
        self.messageOverhead = 0
        self.resultDict = {}

    def funcSocketListening(self,):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.funcPort))
        s.listen(5)
        print('Listening to port ', self.funcPort)
        while True:
            con, addr = s.accept()
            data = con.recv(1024)
            self.processCount += 1
            t = Thread(target=self.mathTask)
            t.start()

    def resultSocketListening(self,):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.resultPort))
        s.listen(5)
        print('Listening to port ', self.resultPort)
        while True:
            con, addr = s.accept()
            data = con.recv(1024)
            self.messageOverhead += 1
            print('Data received from heavy load system ', data)
            result = data.decode('utf-8').split('_')
            print('Reuslt in list ', result[-3])
            self.resultDict[int(result[-3])] = True

    def sum_primes(self, n):
        """Calculates sum of all primes below given integer n"""

        def isprime( x):
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

    def loadSocketListening(self,):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.loadPort))
        s.listen(5)
        print('Listening to port ', self.loadPort)
        while True:
            con, addr = s.accept()
            data = con.recv(1024)
            taskReceivedTime = time.time()
            print('Data received from heavy load system ', data, self.processCount)
            if b'system load' in data:
                con.send(self.processCount.to_bytes(2, 'big'))


    def fibonacci(self, n):
        if n < 2:
            return n
        else:
            return self.fibonacci(n - 1) + self.fibonacci(n - 2)

    def mathTask(self, senderProcessID, con=None, local=False):
        if local:
            num = self.sum_primes(senderProcessID)
            self.processCount -= 1
            self.resultDict[senderProcessID] = True
        else:
            num = self.sum_primes(senderProcessID)
            self.processCount -= 1
            s = socket(AF_INET, SOCK_STREAM)
            s.connect((con, self.resultPort))
            s.sendall(
                b'Result for process ID_' + bytes(senderProcessID, 'utf-8') + b'_is_' + bytes(str(num), 'utf-8'))
            s.close()

if __name__ == '__main__':
    obj = SI39(funcPort=10040, loadPort=10060, resultPort=10070, processCount=0)
    TEXTFLAG = False
    PPFLAG = False
    thread1 = Thread(target=obj.funcSocketListening)
    thread1.start()
    thread2 = Thread(target=obj.loadSocketListening)
    thread2.start()
    thread3 = Thread(target=obj.resultSocketListening)
    thread3.start()
    threshold = 5
    serverDetails = {
                    #1:('131.151.89.158', 11050, 11040),
                    #3:('131.151.89.14', 11060, 11040, 11000),
                    40: ('131.151.243.67', 10060, 10040),
                    38: ('131.151.243.65', 10060, 10040),
                     }  # port, systemload, functiontransfer.
    dataStack = list(np.arange(1000000, 2000000, 100000))
    toatlNoOfProcesses = len(dataStack)
    transferFunction = pkl.dumps(obj.mathTask)
    execStrtTime = time.time()
    while len(dataStack) > 0:
        if obj.processCount >= threshold:
            systemLoads = {}
            for key, value in serverDetails.items():
                socketCon = socket(AF_INET, SOCK_STREAM)
                socketCon.connect((serverDetails[key][0], serverDetails[key][1]))
                socketCon.sendall(b'What is your system load ')
                data = socketCon.recv(1024)
                obj.messageOverhead += len(serverDetails.keys())
                systemLoads[key] = data
                socketCon.close()
            b = list(systemLoads.values())
            c = list(systemLoads.keys())
            minindex = b.index(min(b))
            if int.from_bytes(min(b), 'big') < threshold:
                print('system with less load is ', c[minindex], int.from_bytes(min(b), 'big'))
                if TEXTFLAG:
                    with open('text.txt', 'rb') as f:
                        textData = f.read()
                    startTime = time.time()
                    for i in range(1, 100):
                        socketCon = socket(AF_INET, SOCK_STREAM)
                        socketCon.connect((serverDetails[c[minindex]][0], serverDetails[c[minindex]][2]))
                        if i==99:
                            socketCon.sendall(b'akhil')
                        else:
                            socketCon.sendall(textData)
                        socketCon.close()
                    endTime = time.time()
                    dataStack.pop()
                    with open('metrics39.csv', 'a') as a:
                        csvwriter = csv.writer(a, delimiter=',')
                        csvwriter.writerow(['Latency', 'Transferred to '+str(c[minindex]), round(endTime-startTime, 5)])
                else:
                    socketCon = socket(AF_INET, SOCK_STREAM)
                    socketCon.connect((serverDetails[c[minindex]][0], serverDetails[c[minindex]][2]))
                    obj.resultDict[dataStack[-1]] = False
                    n = int(dataStack[-1])
                    print('Bytes converted ',n,  int.from_bytes(n.to_bytes((n.bit_length() + 7) // 8, 'big'), 'big'))
                    startTime = time.time()
                    socketCon.sendall(transferFunction+ b'_'+ n.to_bytes((n.bit_length() + 7) // 8, 'big'))#dataStack[-1].to_bytes(2,'big'))
                    with open('transfermetrics.csv', 'a') as a:
                        csvwriter = csv.writer(a, delimiter=',')
                        csvwriter.writerow([toatlNoOfProcesses, dataStack[-1], str(c[minindex]), round(time.time()-startTime, 5)])
                    obj.messageOverhead += 1
                    socketCon.close()
                    dataStack.pop()
                    time.sleep(1)
        else:
            print('-------------------------- Assigning task to current system ',dataStack[-1])
            obj.processCount += 1
            obj.resultDict[dataStack[-1]] = False
            t = Thread(target=obj.mathTask, args=(dataStack[-1],None, True))
            dataStack.pop()
            t.start()
    while False in  obj.resultDict.values():
        pass
    execStopTime = time.time()-execStrtTime
    with open('executionmetrics.csv', 'a') as a:
         csvwriter = csv.writer(a, delimiter=',')
         csvwriter.writerow([str(toatlNoOfProcesses), round(execStopTime, 5), obj.messageOverhead])
    print('Waiting for process to be finished',  obj.resultDict.values(), obj.messageOverhead)
    print('Execution completed ', execStopTime)



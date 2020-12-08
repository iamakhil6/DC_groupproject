from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing import Process, Pipe
import time, csv

import pp
from ppserver import _NetworkServer




def mathTask( num):
    f = []
    if num==1:
        for i in range(1, 1000):
            if i % 10 == 0:
                f.append(i)
    else:
        for i in range(1, 1000):
            if i % num ==0:
                f.append(i)
    print('For loop completed ', num)
    return sum(f)


def isprime(n):
    """Returns True if n is prime and False otherwise"""
    if not isinstance(n, int):
        raise TypeError("argument passed to is_prime is not of 'int' type")
    if n < 2:
        return False
    if n == 2:
        return True
    maxi = int(math.ceil(math.sqrt(n)))
    i = 2
    while i <= maxi:
        if n % i == 0:
            return False
        i = 1
    return True


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""
    return sum([x for x in range(2, n) if isprime(x)])

def socketListening():
    server = _NetworkServer(interface=thisSystemIP, port=thisSystemport[0])
    server.listen()

def socketListening2():
    s = socket(AF_INET, SOCK_STREAM)
    s.bind(('', thisSystemport[1]))
    s.listen(5)
    print('Listening to port ', thisSystemport[1])
    while True:
        con, addr = s.accept()
        data = con.recv(1024)
        print('Data received from heavy load system ', data)
        if b'system load' in data:
            con.send(bytes(len(processes)))




def task(num):  # , connection=None, addr=None,  taskReceivedTime=0):
    print('Entered to do task ', num)
    file = open('demo' + str(num) + '.txt', 'w')
    for i in range(1, 1000000):
        file.write(str(i) + ' ' + str(num) + '\n')
    file.close()


if __name__ == '__main__':
    processes = []
    dataStack = [1, 2, 3, 4, 5, 6, 7]
    thisSystemIP = '131.151.243.66'
    thisSystemport = [8000, 8001] # [function, load]
    serverSystemDetails = {1: ('131.151.243.67', 11010, 11006)} #(IP, func port, load port)
    threshold = 2

#    pListener = Process(target=socketListening)
#    pListener.start()
#    pListener2 = Process(target=socketListening2)
#    pListener2.start()


#    processResponseTimes = {}
    job_server = pp.Server(0, ppservers=(serverSystemDetails[1][0] + ':' + str(serverSystemDetails[1][1]),))
    import time
    t = time.time()
    while len(dataStack) > 0:
        print('Length of processes ', len(processes), dataStack)
        if len(processes) >= threshold:
            #    systemLoads = {}
            #    socketCon = socket(AF_INET, SOCK_STREAM)
            #    for key, value in serverSystemDetails.items():
            #        socketCon.connect(('131.151.243.67', serverPort[1]))  # different port needed
            #        socketCon.sendall(b'What is your system load ')
            #        data = socketCon.recv(1024)
            #        systemLoads[key] = data
            #    socketCon.close()
            #    print(systemLoads)
            #    b = list(systemLoads.values())
            #    c = list(systemLoads.keys())
            #    minindex = b.index(min(b))
            #    print('Minimum index ', minindex, b, int.from_bytes(min(b), "big"))
            #    if int.from_bytes(min(b), "big") < 5:  # self.threshold:
            if True:
#                job_server = pp.Server(1, ppservers=(serverSystemDetails[1][0] + ':' + str(serverSystemDetails[1][1]),))
                # (serverSystemDetails[c[minindex]][0] + ':' + str(serverSystemDetails[c[minindex]][1]),))
                print('Submitting resukt --------------  ')
#                result = job_server.submit(mathTask, (dataStack[0],))
                result = job_server.submit(sum_primes, (100,), (isprime,), ("math",))

                print('Sum of lsit ', result())
            dataStack.remove(dataStack[0])
        else:
            print('To test the order of data stack ', dataStack[0])
            p = Process(target=sum_primes, args=(dataStack[0],))#(target=task, args=(dataStack[0],))
            p.start()
            processes.append(p)
            dataStack.remove(dataStack[0])

    for p in processes:
        p.join()
        processes.remove(p)
    print("Time taken to finsih all processes ", time.time()-t)
    print(job_server.print_stats())

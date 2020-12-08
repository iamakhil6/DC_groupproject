from ppserver import _NetworkServer
from multiprocessing import Process
from socket import socket, AF_INET, SOCK_STREAM

thisSystemport = [11010, 11011]
serverPort = 8011
thisSystemIP = '131.151.243.67'
def socketListening():
    server = _NetworkServer(interface=thisSystemIP, port=thisSystemport[0])
    server.listen()

def socketListening2():
    s = socket(AF_INET, SOCK_STREAM)
    s.bind(('', thisSystemport[1]))
    print('Listening to ', thisSystemport[1])
    s.listen(5)
    while True:
        con, addr = s.accept()
        data = con.recv(1024)
        print('Data received for memory verificatin ', data)
        if b'system load' in data:
            con.send(bytes(1))


if __name__ == '__main__':
    pListener = Process(target=socketListening)
    pListener.start()
    pListener2 = Process(target=socketListening2)
    pListener2.start()
    pListener.join()
    pListener2.join()

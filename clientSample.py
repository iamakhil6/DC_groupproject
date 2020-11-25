import sys
from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM

SERVER_IP = '192.168.0.27'
PORT_NUMBER = 5000
SIZE = 1024
print ("Test client sending packets to IP {0}, via port {1}\n".format(SERVER_IP, PORT_NUMBER))

mySocket = socket( AF_INET, SOCK_DGRAM )
# myMessage = 1#"Hello!"
# myMessage1 = ""
i = 0
while i < 10:
    print(i)
    mySocket.sendto(i.to_bytes(2, 'big'),(SERVER_IP,PORT_NUMBER)) #myMessage.encode('utf-8')
    # data, addr = mySocket.recvfrom(1024)
    # print(data)
    i = i + 1

mySocket.close()

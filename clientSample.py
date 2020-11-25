import socket

# Create a socket object
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Define the port on which you want to connect
port = 5000

# connect to the server on local computer
s.sendto(b'1', ('192.168.0.15', port))
data, addr = s.recvfrom(1024)
print(data)
# receive data from the server
# print(s.recv(1024))
# close the connection
s.close()

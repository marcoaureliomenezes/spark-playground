import socket

if __name__ == "__main__":

  HOST = '127.0.0.1'
  PORT = 58000
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect((HOST, PORT))
  print("Connected to server")


  while True:
    request = sock.recv(1024)
    request_dec = request.decode("utf-8")
    print(request_dec)

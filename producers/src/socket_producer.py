from data_generator import DataGenerator
import socket, time, json, os
from datetime import datetime as dt

if __name__ == "__main__":

  server_ip = "socket_producer"
  port = int(os.getenv("PORT"))
  data_generator = DataGenerator(batch_interval=1000, min_batch_size=2, max_batch_size=5, freq=1)
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    sock.bind((server_ip, port))
    sock.listen(0)
  except OSError:
    print("Port already in use")
    sock.close()
    exit()
  
  print("Server started")
  client_socket, client_address = sock.accept()
  print("Client connected")

  date_start = dt.strptime("2020-02-23", "%Y-%m-%d")
  data_generator.run(date_start)
  for msg in data_generator.run(date_start):
    try:
      print(msg)
      msg_to_send = json.dumps(msg) + "\n"
      msg_encoded = msg_to_send.encode('utf-8')
      client_socket.send(msg_encoded)
    except:
      print("Connection closed")
      client_socket.close()
      break


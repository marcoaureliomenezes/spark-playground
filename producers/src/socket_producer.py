from data_generator import gen_data
import socket, time, json, os

if __name__ == "__main__":

  server_ip = "socket_producer"
  port = int(os.getenv("PORT"))
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

  while True:
    try:
      msg = gen_data()
      print(msg)
      msg_to_send = json.dumps(msg) + "\n"
      msg_encoded = msg_to_send.encode('utf-8')
      client_socket.send(msg_encoded)
    except:
      print("Connection closed")
      client_socket.close()
      break
    time.sleep(1)


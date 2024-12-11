import socket

def main():
    
    with socket.create_connection(('localhost',  6379)) as client_socket:
        
        print("Connected to the Server")
        client_socket.sendall(b"*2\r\n$4\r\nECHO\r\n$5\r\napple\r\n")

        response = client_socket.recv(1024)
        
        print("Received from Server to Client2: ", response.decode())

if __name__ == '__main__':
    main()
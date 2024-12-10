import socket

def main():
    
    with socket.create_connection(('localhost',  6379)) as client_socket:

        print("Connected to the Server")
        client_socket.sendall(b"PING")
        
        response = client_socket.recv(1024)
        
        print("Received from Server to Client1: ", response.decode())

if __name__ == '__main__':
    main()
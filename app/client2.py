import socket

def main():
    commands = (
        "*1\r\n$4\r\nPING\r\n"
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
    )

    try:
        with socket.create_connection(('localhost', 6379)) as client_socket:
            print("Connected to the Server")
            
            # Send commands to the server
            client_socket.sendall(commands.encode())

            # Receive full response
            response = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:  # Connection closed or no more data
                    break
                response += chunk
            
            print("Received from Server: ", response.decode())
    
    except ConnectionRefusedError:
        print("Connection to the server failed. Is the server running?")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()

#               Using Event Loop
# __________________________________________________

import asyncio

async def handle_client(reader, writer):
    address = writer.get_extra_info('peername')
    print('Connected to new address: ', address)

    while True:
        data = await reader.read(1024)

        if not data:
            break

        commands = data.decode().upper().strip().split()

        if commands[0] == 'PING' and len(commands) == 1:
            response = b"+PONG\r\n"
        elif commands[0] == 'ECHO' and len(commands) == 2:
            response = commands[1].encode()
        else:
            response = b"-ERR Unknown Command \r\n"
        
        writer.write(response)
        await writer.drain() #Ensure the message is sent
    
    writer.close()
    await writer.wait_closed() #Wait untit the connection is closed

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server is runnin on port 6379...")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

#               Using Thread Method
# __________________________________________________

# import socket  # noqa: F401
# import threading

# def handle_client(connection, address):
#     print("New Connection");
#     while True:
#         data = connection.recv(1024) #Receive the client input
        
#         if not data: #Check if the data is b"" which is empty
#             break
        
#         #Send the hardcode PONG response to the client
#         connection.sendall(b"+PONG\r\n")
#     connection.close()

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")

#     # Uncomment this to pass the first stage
#     server_socket = socket.create_server(("localhost", 6379))

#     while True:
#         connection, address = server_socket.accept() #Wait for client to connect
#         thread = threading.Thread(target=handle_client, args=(connection, address))
#         thread.start()

   
    

# if __name__ == "__main__":
#     main()

#               Using Event Loop
# __________________________________________________

import asyncio
import re

async def handle_client(reader, writer):
    address = writer.get_extra_info('peername')
    print('Connected to new address: ', address)

    while True:
        data = await reader.read(1024)

        if not data:
            break

        command = data.decode()

        pattern = r'\*(\d+)\r\n((?:\$\d+\r\n[^\r\n]+\r\n)+)'

        match = re.match(pattern, command)

        if match:
            args_string = match.group(2)

            args_pattern = r'\$(\d+)\r\n([^\r\n]+)\r\n'

            args = re.findall(args_pattern, args_string)
            arguments = [arg[1] for arg in args]

            if arguments[0].upper().strip() == 'PING' and len(arguments) == 1:
                response = '+PONG\r\n'
            elif arguments[0].upper().strip() == 'ECHO' and len(arguments) == 2:
                response = arguments[1].strip()
            else:
                response = '+ERR Unknown Command \r\n'

        writer.write(response.encode())
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

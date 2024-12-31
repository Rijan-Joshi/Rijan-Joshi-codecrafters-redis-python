#           Using Event Loop (Using Buffer-Based Incremental Parsing Method)
# ___________________________________________________________________________________________

import asyncio
import sys
import time
import struct
import os

record = {}

config = {"dir": "/tmp", "dbfilename": "dump.rdb"}


async def handle_client(reader, writer):
    address = writer.get_extra_info("peername")
    print("Connection established with the client address: ", address)
    buffer = b""

    while True:
        data = await reader.read(1024)

        if not data:
            break

        buffer += data

        while b"\r\n" in buffer:
            line_end = buffer.find(b"\r\n")
            line = buffer[:line_end].decode()
            print(f"Buffer: {buffer} Line: {line}")
            buffer = buffer[line_end + 2 :]

            if line.startswith("*"):
                args_length = int(line[1:])
                print(f"Length: {args_length}")
                args = []
                for _ in range(args_length):
                    if buffer.startswith(b"$"):
                        end = buffer.find(b"\r\n")
                        bulk_length = int(buffer[1:end])
                        buffer = buffer[end + 2 :]
                        arg = buffer[:bulk_length].decode()
                        buffer = buffer[bulk_length + 2 :]
                        args.append(arg)

                print(f"Arguments: {args}")

                response = handle_command(args)

                writer.write(response.encode())
                await writer.drain()

    writer.close()
    await writer.wait_closed()


def read_length(f) -> int:
    byte = f.read(1)

    first_byte = byte[0]

    if first_byte < 0xC0:
        return first_byte
    elif first_byte == 0xC0:
        next_byte = f.read(1)
        return struct.unpack("B", next_byte)[0]
    elif first_byte == 0xC1:
        next_bytes = f.read(2)
        return struct.unpack("<H", next_bytes)[0]
    elif first_byte == 0xC2:
        next_bytes = f.read(4)
        return struct.unpack("<I", next_bytes)[0]


def read_string(f) -> str:
    length = read_length(f)
    data = f.read(length)
    return data.decode("utf-8")


def load_rdb(filename: str) -> None:
    """Load data from rdb file"""

    """Check for headers, database, and end of file"""

    with open(filename, "rb") as f:
        magic = f.read(5)
        if magic != b"REDIS":
            raise ValueError("Invalid RDB file")

        version = f.read(4)
        key = value = ""
        while True:

            type_byte = f.read(1)
            print("Type Byte: ", type_byte)

            if not type_byte:
                break

            opcode = type_byte[0]

            expiry = None
            if opcode == 0xFF:
                break
            # elif opcode == 0xFA:
            #     name = read_string(f)
            #     value = read_string(f)
            #     print("Name: ", name)
            #     print("Value: ", value)
            #     print("Metadata Passed succesfully")
            #     continue
            elif opcode == 0xFE:
                f.read(1)
                print("Database NUMBER passed successfully")
                continue

            if opcode == 0xFB:
                f.read(2)
                print("Database hash size passed successfully")

                while True:
                    type_byte = f.read(1)
                    opcode = type_byte[0]

                    if opcode == 0xFF:
                        return

                    if opcode == 0xFD:
                        expiry = struct.unpack("<I", f.read(4))[0] * 1000
                        continue

                    if opcode == 0xFC:
                        expiry = struct.unpack("<Q", f.read(8))[0]
                        continue

                    key = read_string(f)
                    value = read_string(f)

                    if (
                        key
                        and value
                        and (expiry is None or expiry > time.time() * 1000)
                    ):
                        record[key] = (value, expiry)


def handle_command(args):
    if args[0].upper() == "PING":
        response = "+PONG\r\n"
    elif args[0].upper() == "ECHO" and len(args) > 1:
        response = f"${len(args[1])}\r\n{args[1]}\r\n"
    elif args[0].upper() == "SET":
        if len(args) < 3:
            response = "-ERR Insufficient arguments in command"
            return response
        else:
            key = args[1]
            value = args[2]
            expiry = None

            if len(args) > 3 and args[3].upper() == "PX" and args[4]:
                # Check if the expiry time is in proper format
                try:
                    # Find the expiry time for key
                    expiry = time.time() * 1000 + int(args[4])
                except ValueError:
                    response = "-ERR Invalid expiration value"
                    return response
                print(f"Expiry: {expiry}")

            record[key] = (value, expiry)
            print(record)
            response = "+OK\r\n"
    elif args[0].upper() == "GET":
        if len(args) < 2:
            response = "$-1\r\n"
        else:
            print("Argument: ", args[1])
            print("Record: ", record)
            print(args[1] in record)
            key = args[1]
            if key in record:
                expiry = record[key][1]
                print(f"Expiry time: {expiry}")
                # Check if expiry is set or the key has been expired or
                if expiry and time.time() * 1000 > expiry:
                    del record[key]
                    response = f"$-1\r\n"
                else:
                    response = f"${len(record[key][0])}\r\n{record[key][0]}\r\n"
            else:
                response = "$-1\r\n"
    elif args[0].upper() == "CONFIG":
        if len(args) < 2:
            response = "-ERR Insufficient arguments in command\r\n"
        else:
            if args[1].upper() == "GET":
                param = args[2]
                if param in config:
                    value = config[param]
                    response = f"*2\r\n${len(param)}\r\n{param}\r\n${len(value)}\r\n{value}\r\n"
                else:
                    response = f"*0\r\n"
    elif args[0].upper() == "KEYS":
        if len(args) < 2:
            response = "-ERR Insufficient arguments in command\r\n"
        else:
            response = f""
            if args[1] == "*":
                keys = record.keys()
                print("Record: ", record)
                print("Temp_Keys: ", keys)
                response += f"*{len(keys)}\r\n"
                response += "\r\n".join([f"${len(key)}\r\n{key}" for key in keys])
                response += "\r\n"
                print("Response: ", response)
    else:
        response = "-ERR Unknown Command\r\n"

    return response


async def main():
    if "--dir" in sys.argv:
        config["dir"] = sys.argv[sys.argv.index("--dir") + 1]
    if "--dbfilename" in sys.argv:
        config["dbfilename"] = sys.argv[sys.argv.index("--dbfilename") + 1]

    if config["dir"] and config["dbfilename"]:
        rdb_path = f"{config['dir']}/{config['dbfilename']}"
        if os.path.exists(rdb_path):
            load_rdb(rdb_path)

    server = await asyncio.start_server(handle_client, "localhost", 6379)

    print("Server is runnning on server 6379...")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

    #               Using Event Loop (Using regex expression)
    # ___________________________________________________________________

    # import asyncio
    # import re

    # async def handle_client(reader, writer):
    #     address = writer.get_extra_info('peername')
    #     print('Connected to new address: ', address)

    #     while True:
    #         data = await reader.read(1024)

    #         if not data:
    #             break

    #         command = data.decode()

    #         #Pattern for RESP command
    #         pattern = r'\*(\d+)\r\n((?:\$\d+\r\n[^\r\n]+\r\n)+)'

    #         match = re.match(pattern, command)

    #         if match:
    #             #Capture the RESP Bulk String
    #             args_string = match.group(2)

    #             #Pattern for RESP Bulk String
    #             args_pattern = r'\$(\d+)\r\n([^\r\n]+)\r\n'

    #             args = re.findall(args_pattern, args_string)
    #             arguments = [arg[1] for arg in args]

    #             if arguments[0].upper().strip() == 'PING' and len(arguments) == 1:
    #                 response = '+PONG\r\n'
    #             elif arguments[0].upper().strip() == 'ECHO' and len(arguments) == 2:
    #                 response = f"${len(arguments[1])}\r\n{arguments[1]}\r\n"
    #             else:
    #                 response = '-ERR Unknown Command \r\n'
    #         else:
    #             response = '-ERR Malformed Request\r\n'

    #         writer.write(response.encode())
    #         await writer.drain() #Ensure the message is sent

    #     writer.close()
    #     await writer.wait_closed() #Wait untit the connection is closed

    # async def main():
    #     server = await asyncio.start_server(handle_client, "localhost", 6379)
    #     print("Server is running on port 6379...")
    #     async with server:
    #         await server.serve_forever()

    # if __name__ == "__main__":
    #     asyncio.run(main())

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
    main()

import asyncio

async def send_command(command):
    reader, writer = await asyncio.open_connection('localhost', 6379)
    
    print(f'Sending command: {command}')
    writer.write(command.encode())
    await writer.drain()
    
    response = await reader.read(1024)
    print(f'Received response: {response.decode()}')
    
    writer.close()
    await writer.wait_closed()

async def main():
    await send_command('*1\r\n$4\r\nPING\r\n')
    await send_command('*2\r\n$4\r\nECHO\r\n$5\r\nHello\r\n')

if __name__ == "__main__":
    asyncio.run(main())
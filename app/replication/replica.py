import asyncio
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder


class RedisReplica:
    def __init__(self, config: RedisServerConfig):
        self.config = config
        self.encoder = RESPEncoder()
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.replicaof["host"], self.config.replicaof["port"]
        )

    async def handle_replication(self):

        # Connect to master server
        await self.connect()

        # Send handshake
        await self.send_handshake(["PING"])
        await self.send_handshake(["REPLCONF", "listening-port", str(self.config.port)])
        await self.send_handshake(["REPLCONF", "capa", "psync2"])

    async def send_handshake(self, command):
        # Send Handshake
        try:
            # Encode and send command
            encoded_command = self.encoder.encode_array(command)
            print(f"Sending command: {command}")
            print(f"Encoded command: {encoded_command}")

            self.writer.write(encoded_command)
            await self.writer.drain()

            # Wait for and process response
            data = await self.reader.read(1024)
            response = data.decode()
            print(f"Received response: {response}")

            return response

        except Exception as e:
            print(f"Error during handshake: {e}")
            raise

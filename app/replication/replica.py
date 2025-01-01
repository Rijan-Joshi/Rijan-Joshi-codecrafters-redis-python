import asyncio
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder


class RedisReplica:
    def __init__(self, config: RedisServerConfig):
        self.config = config
        self.encoder = RESPEncoder()

    async def handle_replication(self):
        command = ["PING"]
        await self.send_handshake(command)

    async def send_handshake(self, command):
        command = self.encoder.encode_array(command)
        print(
            f"Handshake sent",
            self.config.replicaof["host"],
            self.config.replicaof["port"],
        )
        reader, writer = await asyncio.open_connection(
            self.config.replicaof["host"], self.config.replicaof["port"]
        )
        writer.write(command)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

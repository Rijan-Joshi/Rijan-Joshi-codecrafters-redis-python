import asyncio
import logging
import signal
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder

logger = logging.getLogger(__name__)


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

        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

    async def shutdown(self, sig):
        """Gracefully shutdown the server"""
        logger.info(f"Received signal {sig}, shutting down...")
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        asyncio.get_event_loop().stop()

    async def handle_replication(self):
        try:
            # Connect to master server
            await self.connect()

            # Send handshake
            await self.send_handshake(["PING"])
            await self.send_handshake(
                ["REPLCONF", "listening-port", str(self.config.port)]
            )
            await self.send_handshake(["REPLCONF", "capa", "psync2"])
            await self.send_handshake(["PSYNC", "?", "-1"])

        except Exception as e:
            logger.error(f"Error during replication: {e}")
        finally:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
                logger.info("Connection closed")

    async def send_handshake(self, command):
        # Send Handshake
        try:
            # Encode and send command
            encoded_command = self.encoder.encode_array(command)
            logger.info(f"Sending command: {command}")
            logger.info(f"Encoded command: {encoded_command}")

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

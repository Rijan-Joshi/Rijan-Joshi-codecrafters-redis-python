import asyncio
import logging
import signal
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder
from app.protocol.resp_decoder import RESPDecoder
from app.commands.command import CommandHandler
from app.database import DataStore

logger = logging.getLogger(__name__)


class RedisReplica:
    def __init__(self, config: RedisServerConfig, db: DataStore):
        self.config = config
        self.encoder = RESPEncoder()
        self.decoder = RESPDecoder()
        self.db = DataStore(self.config) if db is None else db
        self.command_handler = CommandHandler(self.db, config)
        self.command_queue = asyncio.Queue()
        self.reader = None
        self.writer = None
        self.server_task = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.replicaof["host"], self.config.replicaof["port"]
        )

        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

    async def shutdown(self, sig):
        """Gracefully shutdown the server"""
        logger.info(f"Received signal {sig}, shutting down...")
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        if self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                logger.info("Server task was cancelled")

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        await asyncio.gather(*tasks, return_exceptions=True)

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

            print("Completed handshake")

            # server = RedisServer(self.config, self.db)
            # self.server_task = asyncio.create_task(server.start())

            while True:
                try:
                    command_args_list = await self.decoder.decode(self.reader)
                    if not command_args_list:
                        break
                    for command_args in command_args_list:
                        #     await self.command_queue.put(command_args)

                        # while not self.command_queue.empty():
                        #     command_args = await self.command_queue.get()
                        await self.handle_master_command(command_args)
                except Exception as e:
                    logger.error(f"Error processing command from master: {e}")
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
            try:
                response = data.decode()
            except UnicodeDecodeError:
                response = data
            print(f"Received response: {response}")

            return response

        except Exception as e:
            print(f"Error during handshake: {e}")
            raise

    async def handle_master_command(self, args):
        try:
            response = await self.command_handler.handle_command(args, self.writer)
            logger.info(f"Response {response}")
        except Exception as e:
            logger.error(f"Error handling command: {e}")

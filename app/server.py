import asyncio
import logging
import signal
from app.database import DataStore
from typing import Optional
from app.commands.command import CommandHandler
from app.protocol.RDBLoader import RDBLoader
from app.protocol.resp_decoder import RESPDecoder
from app.utils.config import RedisServerConfig

"""Redis Server"""
# Database
# RESP PARSER
# Command Handler
# Handle Signal
# Handle Buffering Logic

logger = logging.getLogger(__name__)


class RedisServer:
    def __init__(self, config: "RedisServerConfig"):
        self.config = config
        self.database = DataStore()
        self.command_handler = CommandHandler(self.database, config)
        self.server: Optional[asyncio.AbstractServer] = None
        self.resp_decoder = RESPDecoder()

    async def start(self):

        # Load the RDB file if present
        if self.config.rdb_path.exists():
            try:
                RDBLoader.load(self.config.rdb_path, self.database)
                logger.info(f"RDB Load Successful from {self.config.rdb_path}")
            except Exception as e:
                logger.error(f"Failed to load RDB file: {e}")

        # Starting the server and listening for incoming connections
        self.server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )

        # Signal Setup
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

        async with self.server:
            await self.server.serve_forever()

    async def shutdown(self, sig):
        """Gracefully shutdown the server"""
        logger.info(f"Received signal {sig}, shutting down...")
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        asyncio.get_event_loop().stop()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle Individual Client Connection"""

        address = writer.get_extra_info("peername")
        logger.info(f"New Connection from {address}")

        try:
            try:
                while True:
                    command_args = await self.resp_decoder.decode(reader)
                    if not command_args:
                        break
                    print("Command Arguments", command_args)
                    response = await self.command_handler.handle_command(command_args)
                    print("This is response", response)
                    writer.write(response)
                    await writer.drain()
            except asyncio.TimeoutError:
                logger.error(f"Timeout while reading from Peer: {address}")
        except ConnectionError as e:
            logger.error(f"Connection Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection Closed from Peer: {address}")

import asyncio
import logging
import signal
from app.database import DataStore
from typing import Optional
from app.commands.command import CommandHandler
from app.protocol.RDBLoader import RDBLoader
from app.protocol.resp_decoder import RESPDecoder
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder

"""Redis Server"""
# Database
# RESP PARSER
# Command Handler
# Handle Signal
# Handle Buffering Logic

logger = logging.getLogger(__name__)


class RedisServer:
    def __init__(
        self, config: "RedisServerConfig", database: Optional[DataStore] = None
    ):
        self.config = config
        self.database = DataStore(self.config) if database is None else database
        self.server: Optional[asyncio.AbstractServer] = None
        self.resp_decoder = RESPDecoder()
        self.encoder = RESPEncoder()
        self.command_handler = CommandHandler(self.database, config)
        self._asyncio_queue = asyncio.Queue()

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
            asyncio.get_running_loop().add_signal_handler(
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

        if self.database.replicas:
            for replica in self.database.replicas:
                try:
                    replica.close()
                    await replica.wait_closed()
                except Exception as e:
                    logger.warning(f"Failed to close replica: {e}")

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        await asyncio.gather(*tasks, return_exceptions=True)

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

                    command_args_list = await self.resp_decoder.decode(reader)
                    if not command_args_list:
                        break

                    for command_args in command_args_list:
                        #     await self._asyncio_queue.put(command)

                        # while not self._asyncio_queue.empty():
                        #     command_args = await self._asyncio_queue.get()
                        response = await self.command_handler.handle_command(
                            command_args, writer
                        )
                        if response:
                            # if isinstance(response, list):
                            #     for resp in response:
                            #         writer.write(resp)
                            #         await writer.drain()

                            # else:
                            writer.write(response)
                            await writer.drain()

            except asyncio.TimeoutError:
                logger.error(f"Timeout while reading from Peer: {address}")
        except ConnectionError as e:
            logger.error(f"Connection Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            # Close the connection
            if self.database.replicas:
                self.database.replicas.remove(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logger.warning(f"Failed to close writer: {e}")
                logger.info(f"Connection Closed from Peer: {address}")

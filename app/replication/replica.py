import asyncio
import logging
import signal
from app.utils.config import RedisServerConfig
from app.protocol.resp_encoder import RESPEncoder
from app.protocol.resp_decoder import RESPDecoder
from app.commands.command import CommandHandler
from app.database import DataStore
from typing import Optional

logger = logging.getLogger(__name__)


class RedisReplica:
    def __init__(self, config: RedisServerConfig, db: DataStore):
        self.config = config
        self.encoder = RESPEncoder()
        self.decoder = RESPDecoder()
        self.db = db
        self.command_handler = CommandHandler(self.db, config)
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.server_task = None

    async def handle_replication(self):
        """Main replication loop"""
        while True:
            try:
                await self._connect()
                await self._perform_handshakes()
                await self._handle_master_stream()
            except asyncio.CancelledError as e:
                logger.error(f"Replication cancelled with error {e}")
                break
            except Exception as e:
                logger.error(f"Error during replication {e}")
                raise

    async def _connect(self):
        """Establish connection to master with timeout"""
        try:
            logger.info(
                f"Replica is connecting to the master at {self.config.replicaof['host']} at port {self.config.replicaof['port']}"
            )

            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.replicaof["host"], self.config.replicaof["port"]
                ),
                timeout=5.0,
            )

            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(self.shutdown(s))
                )
        except Exception as e:
            logger.error(f"Error connecting to the master due to the error: {e}")

    async def _perform_handshakes(self):
        """Handle the hanshakes"""
        try:
            # Handle PONG Command
            await self._send_command(["PING"])
            await self._read_response()

            # Handle REPLCONF listening-port command
            await self._send_command(
                ["REPLCONF", "listening-port", str(self.config.port)]
            )
            await self._read_response()

            # Handle REPLCONF capa command
            await self._send_command(["REPLCONF", "capa", "psync2"])
            await self._read_response()

            # Handle PSYNC command
            psync_command = ["PSYNC", "?", "-1"]
            await self._send_command(psync_command)
            response = await self._read_response()

            await self._process_psync(response)

        except Exception as e:
            logger.error(f"Error performing handshakes: {e}")
            raise

    async def _handle_master_stream(self):
        """Process the command stream from master"""
        try:
            while True:
                commands = await self.decoder.decode(self.reader)
                if not commands:
                    raise Exception(f"Error decoding")

                for command in commands:
                    await self._process_command(command)

                    # Keep track of the number of bytes of commmand processed
                    total = len(self.encoder.encode_array(command))
                    self.db._replication_data["master_repl_offset"] += total
                    logger.info(
                        f"After counting, the offset is {self.db._replication_data["master_repl_offset"]}"
                    )

        except Exception as e:
            logger.error(f"Error while handling master stream: {e}")
            raise

    async def _send_command(self, command):
        """Send the command to the master"""
        encoded_command = self.encoder.encode_array(command)
        self.writer.write(encoded_command)
        await self.writer.drain()

    async def _read_response(self):
        """Receive the response from the master with timeout"""
        return await asyncio.wait_for(self.reader.readline(), timeout=5.0)

    async def _process_psync(self, response):
        """Process the PSYNC command"""
        try:
            if b"FULLRESYNC" in response:
                replica_parts = response.split()

                # Updating the replication infos whenever we get the response
                self.db._replication_data["master_replid"] = replica_parts[1].decode()
                self.db._replication_data["master_repl_offset"] = int(
                    replica_parts[2].decode()
                )

                # Get the size of RDB
                size_data = await self.reader.readline()
                if not b"$" in size_data:
                    logger.error("Inappropriate format of RDB file")
                    raise Exception("Invalid RDB size format")

                # Extract the size of rdb_file
                rdb_size = int(size_data[1:-2])

                # Extract the content of the rdb_file
                rdb_content = await self.reader.read(rdb_size)
                logger.info(f"The RDB content for the replica is {rdb_content}")
            else:
                raise Exception(f"Unexpected PSYNC Response: {response}")

        except Exception as e:
            logger.error(f"Handling PSYNC emerged with error: {e}")

    async def _process_command(self, command):
        """Handle the command using command_handler"""
        try:
            response = await self.command_handler.handle_command(command, self.writer)
            if response and b"ERR" in response:
                logger.error(f"Error while processing the response: {response}")
        except Exception as e:
            logger.error(f"Error Processing the Commmand: {e}")
            raise

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

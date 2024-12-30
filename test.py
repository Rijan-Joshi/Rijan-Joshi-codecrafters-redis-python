# redis_server/
# │
# ├── config.py
from dataclasses import dataclass
from pathlib import Path
import os
from typing import Optional


@dataclass
class ServerConfig:
    host: str = "localhost"
    port: int = 6379
    dir: str = "/tmp"
    dbfilename: str = "dump.rdb"

    @property
    def rdb_path(self) -> Path:
        return Path(self.dir) / self.dbfilename

    @classmethod
    def from_args(cls, args: list[str]) -> "ServerConfig":
        config = cls()
        if "--dir" in args:
            config.dir = args[args.index("--dir") + 1]
        if "--dbfilename" in args:
            config.dbfilename = args[args.index("--dbfilename") + 1]
        return config


# ├── storage.py
import time
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class DataStore:
    def __init__(self):
        self._data: Dict[str, Tuple[str, Optional[int]]] = {}

    def set(self, key: str, value: str, expiry: Optional[int] = None) -> None:
        """Set a key-value pair with optional expiry (in milliseconds)."""
        self._data[key] = (value, expiry)
        logger.debug(f"Set key '{key}' with value '{value}' and expiry {expiry}")

    def get(self, key: str) -> Optional[str]:
        """Get value for key if it exists and hasn't expired."""
        if key not in self._data:
            return None

        value, expiry = self._data[key]
        if expiry and time.time() * 1000 > expiry:
            del self._data[key]
            logger.debug(f"Key '{key}' has expired")
            return None

        return value

    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        current_time = time.time() * 1000
        valid_keys = [
            key
            for key, (_, expiry) in self._data.items()
            if not expiry or expiry > current_time
        ]
        return valid_keys


# ├── protocol.py
from dataclasses import dataclass
from typing import List, Optional
import struct


@dataclass
class RESPCommand:
    command: str
    args: List[str]


class RESPEncoder:
    @staticmethod
    def encode_simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode()

    @staticmethod
    def encode_error(err: str) -> bytes:
        return f"-ERR {err}\r\n".encode()

    @staticmethod
    def encode_bulk_string(s: Optional[str]) -> bytes:
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode()

    @staticmethod
    def encode_array(items: List[str]) -> bytes:
        if not items:
            return b"*0\r\n"
        response = f"*{len(items)}\r\n"
        for item in items:
            response += f"${len(item)}\r\n{item}\r\n"
        return response.encode()


class RDBLoader:
    @staticmethod
    def read_length(f) -> int:
        byte = f.read(1)
        if not byte:
            raise EOFError("Unexpected end of file")

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

    @staticmethod
    def read_string(f) -> str:
        length = RDBLoader.read_length(f)
        data = f.read(length)
        if len(data) != length:
            raise EOFError("Unexpected end of file while reading string")
        return data.decode("utf-8")

    @classmethod
    def load(cls, filename: str, store: "DataStore") -> None:
        try:
            with open(filename, "rb") as f:
                magic = f.read(5)
                if magic != b"REDIS":
                    raise ValueError("Invalid RDB file format")

                version = f.read(4)  # Skip version for now

                while True:
                    type_byte = f.read(1)
                    if not type_byte:
                        break

                    opcode = type_byte[0]
                    if opcode == 0xFF:
                        break
                    elif opcode == 0xFE:
                        f.read(1)  # Skip database number
                        continue
                    elif opcode == 0xFB:
                        cls._process_database_content(f, store)
                        return

        except (IOError, EOFError) as e:
            logger.error(f"Error loading RDB file: {e}")
            raise

    @classmethod
    def _process_database_content(cls, f, store: "DataStore") -> None:
        f.read(2)  # Skip database hash size

        while True:
            type_byte = f.read(1)
            if not type_byte:
                return

            opcode = type_byte[0]
            if opcode == 0xFF:
                return

            expiry = None
            if opcode == 0xFD:
                expiry = struct.unpack("<I", f.read(4))[0] * 1000
                type_byte = f.read(1)
                opcode = type_byte[0]
            elif opcode == 0xFC:
                expiry = struct.unpack("<Q", f.read(8))[0]
                type_byte = f.read(1)
                opcode = type_byte[0]

            key = cls.read_string(f)
            value = cls.read_string(f)

            if expiry is None or expiry > time.time() * 1000:
                store.set(key, value, expiry)


# ├── commands.py
from typing import Optional
import time
from .protocol import RESPEncoder


class CommandHandler:
    def __init__(self, store: "DataStore", config: "ServerConfig"):
        self.store = store
        self.config = config

    def handle_command(self, args: list[str]) -> bytes:
        if not args:
            return RESPEncoder.encode_error("Empty command")

        command = args[0].upper()
        handler = getattr(self, f"handle_{command.lower()}", None)

        if not handler:
            return RESPEncoder.encode_error("Unknown command")

        try:
            return handler(args[1:])
        except Exception as e:
            logger.error(f"Error handling command {command}: {e}")
            return RESPEncoder.encode_error(str(e))

    def handle_ping(self, args: list[str]) -> bytes:
        return RESPEncoder.encode_simple_string("PONG")

    def handle_echo(self, args: list[str]) -> bytes:
        if not args:
            return RESPEncoder.encode_error("Wrong number of arguments")
        return RESPEncoder.encode_bulk_string(args[0])

    def handle_set(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return RESPEncoder.encode_error("Wrong number of arguments")

        key, value = args[0], args[1]
        expiry = None

        if len(args) > 3 and args[2].upper() == "PX":
            try:
                expiry = time.time() * 1000 + int(args[3])
            except ValueError:
                return RESPEncoder.encode_error("Invalid expiration value")

        self.store.set(key, value, expiry)
        return RESPEncoder.encode_simple_string("OK")

    def handle_get(self, args: list[str]) -> bytes:
        if not args:
            return RESPEncoder.encode_bulk_string(None)

        value = self.store.get(args[0])
        return RESPEncoder.encode_bulk_string(value)

    def handle_keys(self, args: list[str]) -> bytes:
        if not args or args[0] != "*":
            return RESPEncoder.encode_error("Invalid pattern")
        return RESPEncoder.encode_array(self.store.keys())

    def handle_config(self, args: list[str]) -> bytes:
        if not args or args[0].upper() != "GET" or len(args) != 2:
            return RESPEncoder.encode_error("Invalid CONFIG command")

        param = args[1]
        if hasattr(self.config, param):
            value = str(getattr(self.config, param))
            return RESPEncoder.encode_array([param, value])
        return RESPEncoder.encode_array([])


# ├── server.py
import asyncio
import logging
from typing import Optional
import signal
import sys


class RedisServer:
    def __init__(self, config: "ServerConfig"):
        self.config = config
        self.store = DataStore()
        self.command_handler = CommandHandler(self.store, config)
        self.server: Optional[asyncio.AbstractServer] = None

    async def start(self):
        """Start the Redis server."""
        if self.config.rdb_path.exists():
            try:
                RDBLoader.load(str(self.config.rdb_path), self.store)
                logger.info(f"Loaded RDB file from {self.config.rdb_path}")
            except Exception as e:
                logger.error(f"Failed to load RDB file: {e}")

        self.server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )

        logger.info(f"Server running on {self.config.host}:{self.config.port}")

        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

        async with self.server:
            await self.server.serve_forever()

    async def shutdown(self, sig: signal.Signals):
        """Gracefully shutdown the server."""
        logger.info(f"Received signal {sig.name}, shutting down...")
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        asyncio.get_event_loop().stop()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle individual client connections."""
        peer = writer.get_extra_info("peername")
        logger.info(f"New connection from {peer}")

        buffer = b""
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                buffer += data
                while b"\r\n" in buffer:
                    line_end = buffer.find(b"\r\n")
                    line = buffer[:line_end].decode()
                    buffer = buffer[line_end + 2 :]

                    if line.startswith("*"):
                        try:
                            args_length = int(line[1:])
                            args = []
                            for _ in range(args_length):
                                if buffer.startswith(b"$"):
                                    end = buffer.find(b"\r\n")
                                    bulk_length = int(buffer[1:end])
                                    buffer = buffer[end + 2 :]
                                    arg = buffer[:bulk_length].decode()
                                    buffer = buffer[bulk_length + 2 :]
                                    args.append(arg)

                            response = self.command_handler.handle_command(args)
                            writer.write(response)
                            await writer.drain()

                        except (ValueError, IndexError) as e:
                            logger.error(f"Error parsing command: {e}")
                            writer.write(RESPEncoder.encode_error("Protocol error"))
                            await writer.drain()

        except Exception as e:
            logger.error(f"Error handling client {peer}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection closed for {peer}")


# ├── __main__.py
import asyncio
import logging
import sys


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("redis_server.log")],
    )


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        config = ServerConfig.from_args(sys.argv[1:])
        server = RedisServer(config)
        asyncio.run(server.start())
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

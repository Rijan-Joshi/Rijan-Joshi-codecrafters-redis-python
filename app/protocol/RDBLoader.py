import struct
import time
import logging
from ..database import DataStore

logger = logging.getLogger(__name__)


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

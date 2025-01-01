import asyncio
import logging

logger = logging.getLogger(__name__)

"""RESP Decoder"""


class RESPDecoder:
    def __init__(self):
        self.buffer = b""

    async def decode(self, reader: asyncio.StreamReader):
        """Decode RESP protocol data from stream"""

        try:
            while True:
                data = await reader.read(4096)

                print("Data", data.decode())
                if not data:
                    break

                self.buffer += data

                while b"\r\n" in self.buffer:
                    print("Stage 1")
                    line_end = self.buffer.find(b"\r\n")
                    line = self.buffer[:line_end].decode()
                    self.buffer = self.buffer[line_end + 2 :]

                    if line.startswith("*"):
                        print("Stage 2")
                        args_length = int(line[1:])
                        args = []
                        print(args_length, "Args Len")
                        for _ in range(args_length):
                            if self.buffer.startswith(b"$"):
                                print("Stage 3")
                                end = self.buffer.find(b"\r\n")
                                bulk_length = int(self.buffer[1:end])
                                self.buffer = self.buffer[end + 2 :]
                                arg = self.buffer[:bulk_length].decode()
                                args.append(arg)
                                self.buffer = self.buffer[bulk_length + 2 :]

                        return args

        except Exception as e:
            logger.error(f"Error decoding the stream")

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
            args_list = []
            while True:
                data = await reader.read(4096)

                if not data:
                    break
                print("Received data", data)

                print("Data", data.decode())

                self.buffer += data

                while b"\r\n" in self.buffer:
                    line_end = self.buffer.find(b"\r\n")
                    if line_end == -1:
                        break
                    line = self.buffer[:line_end].decode()
                    self.buffer = self.buffer[line_end + 2 :]

                    if line.startswith("*"):
                        args_length = int(line[1:])
                        args = []
                        print(args_length, "Args Len")
                        for _ in range(args_length):
                            if self.buffer.startswith(b"$"):
                                end = self.buffer.find(b"\r\n")
                                bulk_length = int(self.buffer[1:end])
                                self.buffer = self.buffer[end + 2 :]
                                arg = self.buffer[:bulk_length].decode()
                                args.append(arg)
                                self.buffer = self.buffer[bulk_length + 2 :]

                        args_list.append(args)
                return args_list

        except Exception as e:
            logger.error(f"Error decoding the stream")

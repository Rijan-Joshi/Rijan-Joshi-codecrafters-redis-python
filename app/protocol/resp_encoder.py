"""RESP Encoder"""

from typing import List


class RESPEncoder:
    @staticmethod
    def encode_simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode()

    @staticmethod
    def encode_error(err: str) -> bytes:
        return f"-ERR {err}\r\n".encode()

    @staticmethod
    def encode_bulk_string(s: str) -> bytes:
        if s is None:
            return b"$-1\r\n"
        s = str(s)
        return f"${len(s)}\r\n{s}\r\n".encode()

    @staticmethod
    def encode_array(items: List[str]) -> bytes:
        print("Items in encode_array", items)
        if not items:
            return b"*0\r\n"
        response = f"*{len(items)}\r\n".encode()
        for item in items:
            if item is None:
                response += "$-1\r\n".encode()
            elif isinstance(item, str):
                response += f"${len(item)}\r\n{item}\r\n".encode()
            elif isinstance(item, bytes):
                response += item
            elif isinstance(item, list):
                response += RESPEncoder.encode_array(item)
        return response

    @staticmethod
    def encode_integer(number: int) -> bytes:
        response = f":{number}\r\n"

        return response.encode()

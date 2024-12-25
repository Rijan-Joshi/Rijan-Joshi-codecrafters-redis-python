import struct
import io

def read_length(f) -> int:
    byte = f.read(1)

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
        return struct.unpack('<I', next_bytes)[0]


def read_string(f) -> str:
    length = read_length(f)
    data = f.read(length)
    return data.decode('utf-8')

data = bytes([0xC0,0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72])

f = io.BytesIO(data)

length = read_string(f)

print(length)
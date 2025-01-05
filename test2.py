response = b"+FULLRESYNC 1 1\r\n$10\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r\n"

if b"FULLRESYNC" in response:
    end = response.find(b"\r\n")
    line = response[:end].decode()
    response = response[end + 2 :]
    len_end = response.find(b"\r\n")
    length = int(response[1:len_end])
    response = response[len_end + 2 :]
    rdb_path = response
    print(rdb_path)

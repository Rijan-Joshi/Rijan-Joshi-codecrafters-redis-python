buffer = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"


def decode(buffer):
    args = []
    while b"\r\n" in buffer:
        line_end = buffer.find(b"\r\n")
        line = buffer[:line_end].decode()
        buffer = buffer[line_end + 2 :]

        if line.startswith("*"):
            args_length = int(line[1:])

            print(args_length, "Args Len")
            a = []
            for _ in range(args_length):
                if buffer.startswith(b"$"):
                    end = buffer.find(b"\r\n")
                    bulk_length = int(buffer[1:end])
                    buffer = buffer[end + 2 :]
                    arg = buffer[:bulk_length].decode()
                    a.append(arg)
                    buffer = buffer[bulk_length + 2 :]
            args.append(a)
    return args


print(decode(buffer))

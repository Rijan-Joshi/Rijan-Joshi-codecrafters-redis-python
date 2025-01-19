from typing import Dict, List, Optional, Tuple
import time
import asyncio


class XRANGECommand(Command):
    """
    XRANGE key start end [COUNT count]
    Example: XRANGE mystream 1516184800000-0 1516184800000-1 COUNT 10
    """

    def __init__(self, args, db: "DataStore", config, should_be_queued, queue, writer):
        super().__init__(args)
        self.db = db
        self.should_be_queued = should_be_queued
        self.queue = queue
        self.writer = writer

    def parse_args(self) -> Tuple[str, str, str, Optional[int]]:
        if len(self.args) < 3:
            raise ValueError("wrong number of arguments for 'XRANGE' command")

        key = self.args[0]
        start = self.args[1]
        end = self.args[2]
        count = None

        # Parse COUNT if present
        if len(self.args) > 3:
            if len(self.args) != 5 or self.args[3].upper() != "COUNT":
                raise ValueError("syntax error")
            count = int(self.args[4])

        return key, start, end, count

    def id_in_range(self, entry_id: str, start: str, end: str) -> bool:
        """Check if entry ID is within range."""
        if start == "-":
            start = "0-0"
        if end == "+":
            end = "9999999999999-9999999999999"
        return start <= entry_id <= end

    async def execute(self):
        try:
            key, start, end, count = self.parse_args()

            # Get stream
            stream = self.db.get(key)
            if stream is None:
                return self.encoder.encode_array([])
            if not isinstance(stream, StreamData):
                return self.encoder.encode_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

            # Filter entries within range
            results = []
            for entry in stream.entries:
                if self.id_in_range(entry.id, start, end):
                    results.append(entry)
                    if count and len(results) >= count:
                        break

            # Encode results
            encoded_entries = []
            for entry in results:
                entry_data = [entry.id]
                for field, value in entry.fields.items():
                    entry_data.extend([field, value])
                encoded_entries.append(entry_data)

            return self.encoder.encode_array(encoded_entries)

        except ValueError as e:
            return self.encoder.encode_error(str(e))
        except Exception as e:
            return self.encoder.encode_error(f"ERR {str(e)}")


class XREADCommand(Command):
    """
    XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
    Example: XREAD COUNT 2 STREAMS mystream 0-0
    """

    def __init__(self, args, db: "DataStore", config, should_be_queued, queue, writer):
        super().__init__(args)
        self.db = db
        self.should_be_queued = should_be_queued
        self.queue = queue
        self.writer = writer

    def parse_args(self) -> Tuple[Optional[int], Optional[int], List[Tuple[str, str]]]:
        args = list(self.args)  # Convert to list for easier manipulation
        count = None
        block = None

        # Parse COUNT if present
        if len(args) >= 2 and args[0].upper() == "COUNT":
            count = int(args[1])
            args = args[2:]

        # Parse BLOCK if present
        if len(args) >= 2 and args[0].upper() == "BLOCK":
            block = int(args[1])
            args = args[2:]

        # Parse STREAMS keyword
        if not args or args[0].upper() != "STREAMS":
            raise ValueError("syntax error")
        args = args[1:]

        # Parse keys and IDs
        if len(args) == 0 or len(args) % 2 != 0:
            raise ValueError("syntax error")

        num_keys = len(args) // 2
        keys = args[:num_keys]
        ids = args[num_keys:]

        return count, block, list(zip(keys, ids))

    async def read_streams(
        self, streams_info: List[Tuple[str, str]], count: Optional[int]
    ) -> Dict[str, List]:
        """Read entries from streams that are newer than the specified IDs."""
        results = {}

        for key, last_id in streams_info:
            stream = self.db.get(key)
            if stream is None:
                continue

            # Get entries newer than last_id
            new_entries = []
            for entry in stream.entries:
                if entry.id > last_id:
                    entry_data = [entry.id]
                    for field, value in entry.fields.items():
                        entry_data.extend([field, value])
                    new_entries.append(entry_data)
                    if count and len(new_entries) >= count:
                        break

            if new_entries:
                results[key] = new_entries

        return results

    async def execute(self):
        try:
            count, block, streams_info = self.parse_args()

            # First attempt to read
            results = await self.read_streams(streams_info, count)

            # If blocking is enabled and no results, wait and retry
            if block is not None and not results:
                start_time = time.time()
                while time.time() - start_time < block / 1000:
                    await asyncio.sleep(0.1)  # Small sleep to prevent CPU hogging
                    results = await self.read_streams(streams_info, count)
                    if results:
                        break

            # Format results for encoding
            encoded_results = []
            for key, entries in results.items():
                encoded_results.append([key, entries])

            return self.encoder.encode_array(encoded_results)

        except ValueError as e:
            return self.encoder.encode_error(str(e))
        except Exception as e:
            return self.encoder.encode_error(f"ERR {str(e)}")

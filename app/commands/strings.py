import asyncio
from .base import Command
from ..database import DataStore
import time
import logging
from app.streams.streamData import StreamData

logger = logging.getLogger(__name__)


class GETCommand(Command):
    def __init__(self, args, db: "DataStore", config):
        super().__init__(args)
        self.db = db

    async def execute(self):
        if len(self.args) < 2:
            self.encoder.encode_error("Need at least 2 arguments")

        value = self.db.get(self.args[1])
        return self.encoder.encode_bulk_string(value)


class SETCommand(Command):
    def __init__(self, args, db: "DataStore", config):
        super().__init__(args)
        self.db = db
        self.config = config

    async def execute(self) -> bytes:
        try:
            if len(self.args) < 3:
                return self.encoder.encode_error("SET requires key and value arguments")

            key, value = self.args[1], self.args[2]
            expiry = None

            if len(self.args) > 3 and self.args[3].upper() == "PX":
                try:
                    px = int(self.args[4])
                    expiry = time.time() * 1000 + px

                except (IndexError, ValueError):
                    return self.encoder.encode_error("Invalid PX value")

            self.db.set(key, value, expiry)
            if self.config.replicaof:
                return

            async with self.db.lock:
                replicas = list(self.db.replicas)

            for replica in replicas:
                try:
                    logger.info("Propagating to replica....")
                    replica.write(self.encoder.encode_array(self.args))
                    await replica.drain()

                    self.db.should_acknowledge = True
                except Exception as e:
                    logger.error(f"Error propagating to replica: {e}")

            return self.encoder.encode_simple_string("OK")
        except Exception as e:
            logger.error(f"Got Error: {e}")
        # finally:
        #     # Propagating as a background process
        #     await self._propagate_to_replicas()

    # async def _propagate_to_replicas(self):
    #     tasks = [self._propagate_to_replica(replica) for replica in self.db.replicas]
    #     try:
    #         await asyncio.gather(*tasks)
    #     except Exception as e:
    #         logger.error("Error propagating")
    #         raise

    # async def _propagate_to_replica(self, replica):
    #     try:
    #         logger.info("Propagating to replica....")
    #         replica.write(self.encoder.encode_array(self.args))
    #         await replica.drain()

    #     except Exception as e:
    #         logger.error(f"Error propagating to replica: {e}")
    #         raise


class KEYSCommand(Command):
    def __init__(self, args, db: "DataStore", config):
        super().__init__(args)
        self.db = db

    async def execute(self) -> bytes:
        print("Ariguments in args", self.args)
        print("Length of keys", self.db.keys())
        if len(self.args) < 2:
            return self.encoder.encode_error("Invalid Pattern")
        else:
            return self.encoder.encode_array(self.db.keys())


class INFOCommand(Command):
    def __init__(self, args, db: "DataStore", config):
        super().__init__(args)
        self.db = db

    async def execute(self) -> bytes:
        if len(self.args) < 2:
            self.encoder.encode_error("INFO requires an argument")
        else:
            return self.encoder.encode_bulk_string(self.db.info())


class INCRCommand(Command):
    def __init__(self, args, db: DataStore, config):
        super().__init__(args)
        self.db = db

    async def execute(self):
        if len(self.args) < 2:
            raise ValueError(f"INCRCommand requires at least one argument: key.")

        try:
            key = self.args[1]
            expiry = None

            if not self.db.get(key):
                value = 1
                self.db.set(key, value, expiry)
                return self.encoder.encode_integer(value)

            value = self.db.get(key)
            print("The value is ", value)

            try:
                result = int(value) + 1
                self.db.set(key, result, expiry)
                return self.encoder.encode_integer(result)
            except Exception:
                return self.encoder.encode_error(
                    "value is not an integer or out of range"
                )

        except Exception as e:
            logger.error(f"Handling INCRCommand got an error: {e}")


class MULTICommand(Command):
    def __init__(self, args, db: DataStore, config):
        super().__init__(args)

    async def execute(self):
        return self.encoder.encode_simple_string("OK")


class EXECCommand(Command):
    def __init__(self, args, db: DataStore, config, should_be_queued, queue):
        super().__init__(args)
        self.should_be_queued = should_be_queued
        self.queue = queue

    async def execute(self):
        if self.should_be_queued == False:
            return self.encoder.encode_error(f"EXEC without MULTI")

        if self.queue.empty():
            return self.encoder.encode_array(None)

        responses = []
        while True:
            if self.queue.empty():
                break
            task = await self.queue.get()
            response = await task
            responses.append(response)
            self.queue.task_done()

        print(self.encoder.encode_array(responses), "Final Result")
        return self.encoder.encode_array(responses)


class DISCARDCommand(Command):
    def __init__(self, args, config, db, should_be_queued, queue):
        super().__init__(args)
        self.should_be_queued = should_be_queued
        self.queue = queue

    async def execute(self):

        if not self.should_be_queued:
            return self.encoder.encode_error(f"DISCARD without MULTI")

        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        return self.encoder.encode_simple_string("OK")


class TYPECommand(Command):
    """Returns the type of value for the key stored in the database"""

    def __init__(self, args, db: DataStore, config):
        super().__init__(args)
        self.db = db

    async def execute(self):
        if len(self.args) < 2:
            raise ValueError(f"TYPECommand requires at least one argument: some_key")

        key = self.args[1]
        value = self.db.get(key)

        if value is None:
            return self.encoder.encode_simple_string("none")

        if isinstance(value, str):
            return self.encoder.encode_simple_string("string")

        if isinstance(value, StreamData):
            return self.encoder.encode_simple_string("stream")


class XADDCommand(Command):
    """Adding Data to the stream"""

    def __init__(self, args, db: DataStore, config):
        super().__init__(args)
        self.db = db

    def parse_entry(self):
        """Parse XADD arugments"""
        _, key, entry_id, *data = self.args

        fields = {}
        for i in range(0, len(data), 2):
            key_ = data[i]
            value = data[i + 1]
            fields[key_] = value

        return key, entry_id, fields

    async def execute(self):
        """Add functionality to XADD Command"""
        try:
            key, id, fields = self.parse_entry()

            stream = self.db.get(key)
            if stream is None:
                stream = StreamData()
                self.db.set(key, stream)
            elif not isinstance(stream, StreamData):
                return self.encoder.encode_error(
                    "WRONGTYPE operation with the key holding data type other than stream"
                )

            try:
                new_entry = stream.add_entry(id, fields)
                return new_entry
            except Exception as e:
                return self.encoder.encode_error(str(e))
        except Exception as e:
            raise ValueError(f"Error executing the XADD Command: {e}")


class XRANGECommand(Command):
    """
    XRANGE key start end [COUNT count]
    """

    def __init__(self, args, db: DataStore, config):
        super().__init__(args)
        self.db = db

    def parse_args(self):
        """Parse XRANGE Arguments"""
        _, key, *data = self.args

        start = data[0]
        end = data[1]

        return key, start, end

    async def execute(self):
        """Execute the XRANGE Command"""
        key, start, end = self.parse_args()

        # Get Stream
        stream = self.db.get(key)
        if stream is None:
            return self.encoder.encode_error(f"Key not found in the database")
        elif not isinstance(stream, StreamData):
            raise ValueError(
                f"WRONGTYPE operation with the key having value of data-type other than stream"
            )

        try:
            data = await stream.execute_xrange(start, end)
            if data:
                return self.encoder.encode_array(data)
        except Exception as e:
            logger.error(f"Got error while getting XRANGE stream: {e}")


class XREADCommand(Command):
    """
    XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
    Example: XREAD COUNT 2 STREAMS mystream 0-0
    """

    def __init__(self, args, db: DataStore, config):
        super().__init__(args)
        self.db = db

    async def process_stream(self, stream_infos):
        """Process the results"""

        results = {}

        for key, id in stream_infos:
            # Get Key
            stream = self.db.get(key)
            if stream is None or not isinstance(stream, StreamData):
                continue

            try:
                data = await stream.execute_xrange(id, "+", True)
                if data:
                    results[key] = data
            except Exception as e:
                logger.error(f"Got error while getting XREAD stream: {e}")

        return results

    async def execute(self):
        """Execute the XREAD Command"""

        block = None
        # Get block
        if "block" in self.args:
            block_index = self.args.index("block")
            block = int(self.args[block_index + 1])

        # Get the index from where the keys start
        stream_index = self.args.index("streams")
        datae = self.args[stream_index + 1 :]
        k = len(datae)

        # Get stream_infos
        keys = datae[: k // 2]
        ids = datae[k // 2 :]
        stream_infos = list(zip(keys, ids))

        # Get the results once at first
        results = await self.process_stream(stream_infos)

        # Execute the block functionality if it exists
        if block is not None:
            try:
                start_time = time.time()
                while time.time() - start_time < block / 1000:
                    await asyncio.sleep(0.1)  # Small sleep to prevent CPU hogging
                    results = await self.process_stream(stream_infos)
            except Exception as e:
                logger.error(f"Error while blocking: {e}")

        outcome = []
        if results:
            for key, entry in results.items():
                outcome.append([key, entry])

        if not outcome:
            return self.encoder.encode_bulk_string(None)
        return self.encoder.encode_array(outcome)

        # Start the encoded
        # results = []

        # for i in range(len(keys)):
        #     key = keys[i]
        #     id = ids[i]

        #     # Get Key
        #     stream = self.db.get(key)
        #     if stream is None:
        #         return self.encoder.encode_error(f"Key not found in the database")
        #     elif not isinstance(stream, StreamData):
        #         raise ValueError(
        #             f"WRONGTYPE operation with the key having value of data-type other than stream"
        #         )

        #     try:
        #         xread = True
        #         data = await stream.execute_xrange(id, "+", xread)
        #         if data:
        #             results.append([key, data])
        #     except Exception as e:
        #         logger.error(f"Got error while getting XREAD stream: {e}")
        #         raise

        # return self.encoder.encode_array(results)

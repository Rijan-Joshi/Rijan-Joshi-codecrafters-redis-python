import asyncio
from .base import Command
from ..database import DataStore
import time
import logging

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

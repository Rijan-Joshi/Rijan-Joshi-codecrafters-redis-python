from .base import Command
from ..database import DataStore
import time
import asyncio
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

            return self.encoder.encode_simple_string("OK")
        finally:
            # Propagating as a background process
            asyncio.create_task(self._propagate_to_replicas())

    async def _propagate_to_replicas(self):
        tasks = [self._propagate_to_replica(replica) for replica in self.db.replicas]
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error("Error propagating")
            raise

    async def _propagate_to_replica(self, replica):
        logger.info("Propagating to replica....")
        replica.write(self.encoder.encode_array(self.args))
        await replica.drain()


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


# Command Support for REDIS Replication Feature
class INFOCommand(Command):
    def __init__(self, args, db: "DataStore", config):
        super().__init__(args)
        self.db = db

    async def execute(self) -> bytes:
        if len(self.args) < 2:
            self.encoder.encode_error("INFO requires an argument")
        else:
            return self.encoder.encode_bulk_string(self.db.info())

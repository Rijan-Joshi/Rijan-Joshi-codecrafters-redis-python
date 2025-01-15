import time
import asyncio
import logging
from .base import Command
from app.utils.config import RedisServerConfig
from app.database import DataStore

logger = logging.getLogger(__name__)


class REPLCONFCommand(Command):
    def __init__(
        self,
        args,
        db: DataStore,
        config: "RedisServerConfig",
        writer: asyncio.StreamWriter = None,
    ):
        super().__init__(args)
        self.db = db
        self.config = config
        self.writer = writer

    async def execute(self):
        print("Okay, I am here.")

        if len(self.args) > 2 and self.args[1].upper() == "GETACK":
            if self.args[2] == "*":
                self.writer.write(
                    self.encoder.encode_array(
                        [
                            "REPLCONF",
                            "ACK",
                            str(self.db._replication_data["master_repl_offset"]),
                        ]
                    )
                )
                await self.writer.drain()
        elif len(self.args) > 2 and self.args[1].upper() == "ACK":
            print("Have I entered here?")
            self.db.ack_replicas[id(self.writer)] = "acknowledged"
        else:
            return self.encoder.encode_simple_string("OK")


class PSYNCCommand(Command):
    def __init__(self, args, db: "DataStore", config, writer: asyncio.StreamWriter):
        super().__init__(args)
        self.db = db
        self.writer = writer

    def _resynchronize(self):
        content = bytes.fromhex(self.db._dummy_empty_rdb)

        return f"${len(content)}\r\n".encode() + content

    async def execute(self):
        response = self.encoder.encode_simple_string(
            f"FULLRESYNC {self.db._replication_data.get("master_replid")} {self.db._replication_data.get("master_repl_offset")}"
        )

        self.writer.write(response)
        await self.writer.drain()
        rdb_content = self._resynchronize()

        self.writer.write(rdb_content)
        await self.writer.drain()

        self.db.replicas.add(self.writer)
        self.db.ack_replicas[id(self.writer)] = None

        return None


class WAITCommand(Command):
    def __init__(
        self, args, db: DataStore, config, writer: asyncio.StreamWriter = None
    ):
        super().__init__(args)
        self.config = config
        self.db = db
        self.writer = writer
        logger.info("WAITCommand initialized with writer: %s", writer is not None)

    async def execute(self):
        if len(self.args) < 3:
            raise ValueError(
                "WAIT command requires exactly two arguments: numreplicas and timeout"
            )

        num_replicas = int(self.args[1])
        timeout = int(self.args[2])
        start_time = time.time()

        for key in self.db.ack_replicas:
            self.db.ack_replicas[key] = None

        while (time.time() - start_time) * 1000 < timeout:
            replicas = list(self.db.replicas)
            for replica in replicas:
                try:
                    print(self.db.ack_replicas)
                    ack_cmd = self.encoder.encode_array(["REPLCONF", "GETACK", "*"])
                    replica.write(ack_cmd)
                    await replica.drain()
                except Exception as e:
                    logger.error(f"Failed to get ACK from replica with error: {e}")
                    if replica in self.db.replicas:
                        self.db.replicas.remove(replica)
                    if id(replica) in self.db.ack_replicas:
                        del self.db.ack_replicas[id(replica)]

            ack_count = sum(1 for ack in self.db.ack_replicas if ack == "acknowledged")
            if ack_count >= num_replicas:
                return self.encoder.encode_integer(ack_count)

            await asyncio.sleep(0.1)

        result = sum(
            1 for ack in self.db.ack_replicas.values() if ack == "acknowledged"
        )
        print("The result is ", result)
        return self.encoder.encode_integer(result)

import asyncio
from .base import Command
from app.utils.config import RedisServerConfig
from app.database import DataStore


class REPLCONFCommand(Command):
    def __init__(self, args, db, config: "RedisServerConfig"):
        super().__init__(args)
        self.config = config

    async def execute(self):
        print("Okay, I am here.")
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

        return None

"""Command Module"""

import asyncio
import logging
from app.database import DataStore
from app.utils.config import RedisServerConfig
from .connection import PINGCommand, ECHOCommand
from app.protocol.resp_encoder import RESPEncoder
from .strings import (
    GETCommand,
    SETCommand,
    KEYSCommand,
    INFOCommand,
    INCRCommand,
    MULTICommand,
    EXECCommand,
)
from .server import ConfigCommand
from .replication import REPLCONFCommand, PSYNCCommand, WAITCommand
from .base import Command


class CommandHandler:
    def __init__(self, db: "DataStore", config: "RedisServerConfig"):
        self.db = db
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.encoder = RESPEncoder()
        self.should_be_queued = False
        self.command_queue = asyncio.Queue()
        self.write_commmands = ["SET", "INCR"]
        self._setup_commands()

    def _setup_commands(self):
        self.commands = {
            "PING": PINGCommand,
            "ECHO": ECHOCommand,
            "GET": GETCommand,
            "SET": SETCommand,
            "KEYS": KEYSCommand,
            "CONFIG": ConfigCommand,
            "INFO": INFOCommand,
            "REPLCONF": REPLCONFCommand,
            "PSYNC": PSYNCCommand,
            "WAIT": WAITCommand,
            "INCR": INCRCommand,
            "MULTI": MULTICommand,
            "EXEC": EXECCommand,
        }

    async def handle_command(self, args, writer=None):

        command_name = args[0].upper()

        if len(args) > 2:
            if args[0] == "-p" and args[1]:
                command_name = args[2].upper()
                args = args[2:]

        print("Arguments", args)
        command_class = self.commands.get(command_name)

        if not command_class:
            logging.error(f"Command not found: {command_name}")
            return self.encoder.encode_error(f"Command not found: {command_name}")

        # Handle write commands on the basis of whether they should be queued or not
        if command_name in self.write_commmands:
            command = command_class(args, self.db, self.config)
            if self.should_be_queued:
                await self.command_queue.put(command.execute)
                return self.encoder.encode_simple_string("QUEUED")

        # Handle all the commands
        if command_name in ["PSYNC", "REPLCONF", "WAIT"]:
            command = command_class(args, self.db, self.config, writer)
        elif command_name == "MULTI":
            self.should_be_queued = True
            command = command_class(args, self.db, self.config)
            return await command.execute()
        elif command_name == "EXEC":
            command = command_class(
                args, self.db, self.config, self.should_be_queued, self.command_queue
            )
            self.should_be_queued = False
            return await command.execute()
        else:
            command = command_class(args, self.db, self.config)

        return await command.execute()

"""Command Module"""

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
    DISCARDCommand,
    TYPECommand,
    XADDCommand,
)
from .server import ConfigCommand
from .replication import REPLCONFCommand, PSYNCCommand, WAITCommand


class CommandHandler:
    def __init__(self, db: "DataStore", config: "RedisServerConfig"):
        self.db = db
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.encoder = RESPEncoder()
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
            "DISCARD": DISCARDCommand,
            "TYPE": TYPECommand,
            "XADD": XADDCommand,
        }

    async def handle_command(self, args, command_state, writer=None):

        command_name = args[0].upper()

        if len(args) > 2:
            if args[0] == "-p" and args[1]:
                command_name = args[2].upper()
                args = args[2:]

        print("Arguments", args)
        command_class = self.commands.get(command_name)

        # Handle Error if command is not found
        if not command_class:
            logging.error(f"Command not found: {command_name}")
            return self.encoder.encode_error(f"Command not found: {command_name}")

        # Handle MULTI Command
        if command_name == "MULTI":
            command_state.should_be_queued = True
            command = command_class(args, self.db, self.config)
            return await command.execute()

        # Handle EXEC Command
        if command_name in ("EXEC", "DISCARD"):
            command = command_class(
                args,
                self.db,
                self.config,
                command_state.should_be_queued,
                command_state.command_queue,
            )
            command_state.should_be_queued = False
            return await command.execute()

        # Handle PSYNC, REPLCONF, WAIT, etc.. command
        if command_name in ["PSYNC", "REPLCONF", "WAIT"]:
            command = command_class(
                args,
                self.db,
                self.config,
                writer,
            )
        # Handle all other commands
        else:
            command = command_class(args, self.db, self.config)

        # Handle the commands if the MULTI command has been sent before
        if command_state.should_be_queued:
            await command_state.command_queue.put(command.execute())
            return self.encoder.encode_simple_string("QUEUED")
        else:
            return await command.execute()

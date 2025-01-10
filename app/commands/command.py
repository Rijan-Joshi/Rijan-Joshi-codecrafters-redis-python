"""Command Module"""

import logging
from app.database import DataStore
from app.utils.config import RedisServerConfig
from .connection import PINGCommand, ECHOCommand
from app.protocol.resp_encoder import RESPEncoder
from .strings import GETCommand, SETCommand, KEYSCommand, INFOCommand
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

        if command_name in ["PSYNC", "REPLCONF"]:
            command = command_class(args, self.db, self.config, writer)
        else:
            command = command_class(args, self.db, self.config)
        return await command.execute()

"""Command Module"""

import logging
from app.database import DataStore
from app.utils.config import RedisServerConfig
from .connection import PINGCommand, ECHOCommand
from app.protocol.resp_encoder import RESPEncoder
from .strings import GETCommand, SETCommand, KEYSCommand, INFOCommand
from .server import ConfigCommand


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
        }

    async def handle_command(self, args):

        command_name = args[0].upper()

        if len(args) > 2:
            if args[0] == "-p" and args[1]:
                command_name = args[2].upper()
                args = args[2:]

        command_class = self.commands.get(command_name)
        print("Command Class", command_class)

        if not command_class:
            logging.error(f"Command not found: {command_name}")
            return self.encoder.encode_error(f"Command not found: {command_name}")

        command = command_class(args, self.db, self.config)
        return await command.execute()

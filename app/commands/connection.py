"""Setup Commands for Connection Handling"""

from .base import Command


class PINGCommand(Command):
    def __init__(self, args, db, config):
        super().__init__(args)

    async def execute(self):
        return self.encoder.encode_simple_string("PONG")


class ECHOCommand(Command):
    def __init__(self, args, db, config):
        super().__init__(args)

    async def execute(self):
        if len(self.args) < 2:
            return self.encoder.encode_error("ECHO command requires an argument")
        return self.encoder.encode_bulk_string(self.args[1])

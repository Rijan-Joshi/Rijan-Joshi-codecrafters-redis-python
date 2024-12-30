from .base import Command
from app.utils.config import RedisServerConfig


class ConfigCommand(Command):

    def __init__(self, args, db, config: "RedisServerConfig"):
        super().__init__(args)
        self.config = config

    async def execute(self) -> bytes:

        if len(self.args) < 3:
            return self.encoder.encode_error("CONFIG GET requires parameter name")

        if self.args[1].upper() != "GET":
            return self.encoder.encode_error("Only CONFIG GET is supported")

        param = self.args[2]
        if hasattr(self.config, param):
            value = getattr(self.config, param, "")
            return self.encoder.encode_array([param, str(value)])
        return self.encoder.encode_error([])

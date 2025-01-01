# from .base import Command
# from app.utils.config import RedisServerConfig


# class REPLCONFCommand(Command):
#     def __init__(self, args, db, config: "RedisServerConfig"):
#         super().__init__(args)
#         self.config = config

#     async def execute(self):
#         print("Okay, I am here.")
#         return self.encoder.encode_simple_string("OK")

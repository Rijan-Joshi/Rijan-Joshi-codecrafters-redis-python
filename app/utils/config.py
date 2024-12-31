from dataclasses import dataclass
from pathlib import Path
import argparse


"""All configuration settings for REDIS"""


@dataclass
class RedisServerConfig:
    host: str = "localhost"
    port: int = 6379
    dir: str = "/tmp"
    dbfilename: str = "dump.rdb"

    @property
    def rdb_path(self):
        return Path(self.dir) / self.dbfilename

    @classmethod
    def parse_args(cls, args):
        config = cls

        # Using argparse to parse the arguments
        parser = argparse.ArgumentParser(description="Redis Server Configuration")
        parser.add_argument(
            "--dir", help="Directory to store RDB file", default=config.dir
        )
        parser.add_argument(
            "--dbfilename", help="Filename of RDB file", default=config.dbfilename
        )
        parser.add_argument(
            "--port", help="Port Number for Custom Redis Server", default=config.port
        )
        parsed_args = parser.parse_args(args)
        return config(
            dir=parsed_args.dir,
            dbfilename=parsed_args.dbfilename,
            port=parsed_args.port,
        )

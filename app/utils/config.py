from dataclasses import dataclass
from pathlib import Path
import argparse
import logging

"""All configuration settings for REDIS"""

logger = logging.getLogger(__name__)


@dataclass
class RedisServerConfig:
    host: str = "localhost"
    port: int = 6379
    dir: str = "/tmp"
    dbfilename: str = "dump.rdb"
    replicaof: dict = None

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
        parser.add_argument(
            "--replicaof",
            help="Replicate the data from the master",
            required=False,
        )
        parsed_args = parser.parse_args(args)

        replicaof = None
        if parsed_args.replicaof:
            try:
                host, port = parsed_args.replicaof.split()
                replicaof = {"host": host, "port": int(port)}
            except Exception as e:
                logger.error(f"Error in parsing replicaof: {e}")
                raise ValueError("--replicaof must be in the format 'host:port'")

        return config(
            dir=parsed_args.dir,
            dbfilename=parsed_args.dbfilename,
            port=int(parsed_args.port),
            replicaof=replicaof,
        )

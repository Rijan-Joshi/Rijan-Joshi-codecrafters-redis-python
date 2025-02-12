"""Project: Build Your Own Redis"""

import asyncio
import logging
import sys
from app.utils.config import RedisServerConfig
from app.server import RedisServer

"""Logging Setup Configuration"""


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("redis_server.log")],
    )


def main():
    """Core Function of the Redis Server Implementation"""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        config = RedisServerConfig.parse_args(args=sys.argv[1:])
        print("Configuration", config)

        server = RedisServer(config)
        asyncio.run(server.start())
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

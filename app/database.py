"""Database for my REDIS"""

import secrets
import string
import time
import logging
from typing import Dict, Tuple, Optional
from app.utils.config import RedisServerConfig
import asyncio
from app.protocol.resp_encoder import RESPEncoder

logger = logging.getLogger(__name__)


class DataStore:
    def __init__(self, config: "RedisServerConfig"):
        self.config = config
        self.encoder = RESPEncoder()
        self._data: Dict[str, Tuple[str, Optional[int]]] = {}
        self.replicas = set()
        self._replication_data = {
            "role": "master",
            "master_replid": self._generate_secure_random_string(),
            "master_repl_offset": 0,
        }
        self._dummy_empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        self._update_replication_data()

    def _generate_secure_random_string(self, length: int = 40) -> str:
        characters = string.ascii_letters + string.digits
        secure_random_string = "".join(
            secrets.choice(characters) for _ in range(length)
        )
        return secure_random_string

    def _update_replication_data(self):
        if self.config.replicaof is not None:
            self._replication_data["role"] = "slave"

    def set(self, key: str, value: str, expiry: Optional[int] = None) -> None:
        """Set a key-value pair with optional expiry (in milliseconds)."""
        self._data[key] = (value, expiry)
        logger.debug(f"Set key '{key}' with value '{value}' and expiry {expiry}")

    def get(self, key: str) -> Optional[str]:
        """Get value for key if it exists and hasn't expired."""
        if key not in self._data:
            return None

        value, expiry = self._data[key]
        if expiry and time.time() * 1000 > expiry:
            del self._data[key]
            logger.debug(f"Key '{key}' has expired")
            return None

        return value

    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        current_time = time.time() * 1000
        valid_keys = [
            key
            for key, (_, expiry) in self._data.items()
            if not expiry or expiry > current_time
        ]
        return valid_keys

    def info(self) -> dict:
        """Return the replication Info"""
        line = ["# Replication"]

        for key, value in self._replication_data.items():
            line.append(f"{key}:{value}")

        return "\n".join(line)

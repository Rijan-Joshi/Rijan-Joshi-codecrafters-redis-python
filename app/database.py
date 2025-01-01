"""Database for my REDIS"""

import time
import logging
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)


class DataStore:
    def __init__(self):
        self._data: Dict[str, Tuple[str, Optional[int]]] = {}
        self._replication_data = {"role": "master"}

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

    @property
    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        current_time = time.time() * 1000
        valid_keys = [
            key
            for key, (_, expiry) in self._data.items()
            if not expiry or expiry > current_time
        ]
        return valid_keys

    @property
    def info(self) -> dict:
        """Return the replication Info"""
        line = ["# Replication"]

        line.append(f"role:{self._replication_data['role']}")

        print("\n".join(line))
        return "\n".join(line)

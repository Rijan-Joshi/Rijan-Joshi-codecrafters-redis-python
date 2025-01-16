"""Setup Command Base Class"""

import asyncio
from abc import ABC, abstractmethod
from typing import List, Optional
from app.protocol.resp_encoder import RESPEncoder


class Command(ABC):
    def __init__(self, args: List[str]):
        self.args = args
        self.encoder = RESPEncoder()

    @abstractmethod
    def execute(self) -> Optional[bytes]:
        pass

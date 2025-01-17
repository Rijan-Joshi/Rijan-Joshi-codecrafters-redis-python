import asyncio
from dataclasses import dataclass


@dataclass
class CommandState:
    should_be_queued: bool = False
    command_queue: asyncio.Queue = asyncio.Queue()

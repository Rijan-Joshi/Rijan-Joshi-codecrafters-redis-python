import asyncio
from dataclasses import dataclass


@dataclass
class CommandState:
    should_be_queued: bool = False
    command_queue: asyncio.Queue = None

    def __post_init__(self):
        # Create a new queue for each instance

        if self.command_queue is None:
            self.command_queue = asyncio.Queue()

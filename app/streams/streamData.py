"""Implementing the logic of Stream Database in Redis (Basic not based on Radix Trie)"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from app.protocol.resp_encoder import RESPEncoder


@dataclass
class StreamEntry:
    """Basic Format for each entry of stream"""

    id: str
    fields: Dict[str, str]


class StreamData:
    """Storing the data for each stream"""

    def __init__(self):
        self.entries: List[StreamEntry] = []
        self.last_timestamp = 0
        self.last_sequence = 0
        self.encoder = RESPEncoder()

    def _validate(self, id):
        """Validate the entry_id"""

        # Unpack the time and sequence
        time, sequence = id.split("-")
        time = int(time)
        if sequence != "*":
            sequence = int(sequence)

        # Respond with error if the id is 0-0
        if time == 0 and sequence == 0:
            return self.encoder.encode_error(
                "The ID specified in XADD must be greater than 0-0"
            )

        # Validate if current time is greater than the last timestamp
        if self.last_timestamp < time:
            if sequence == "*":
                self.last_sequence = 0
            else:
                self.last_sequence = sequence
            self.last_timestamp = time
            return "validated"

        # Validate if current time is equal to the last timestamp
        if self.last_timestamp == time:
            if sequence == "*":
                if time == 0:
                    self.last_sequence = 1
                    return "validated"
                else:
                    self.last_sequence += 1
                    return "validated"

            if self.last_sequence < sequence:
                self.last_sequence = sequence
                return "validated"

        # Respond with error if id doesn't fulfill the criteria
        return self.encoder.encode_error(
            "The ID specified in XADD is equal or smaller than the target stream top item"
        )

    def _generate_id(self):
        """Generate the entry_id if needed"""
        current_unix_time_ms = int(time.time()) * 1000
        sequence = 0

        return f"{current_unix_time_ms}-{sequence}"

    def add_entry(self, entry_id: Optional[str], fields: Dict[str, str]) -> str:
        """Add a new entry to the stream"""
        if entry_id is None or entry_id == "*":
            entry_id = self._generate_id()
            if entry_id == f"{self.last_timestamp}-{self.last_sequence}":
                entry_id = f"{self.last_timestamp}-*"

        # Validated the entry id
        validation = self._validate(entry_id)

        if validation == "validated":
            entry = StreamEntry(id=entry_id, fields=fields)
            self.entries.append(entry)
            print(
                "The result is",
                self.encoder.encode_simple_string(
                    f"{self.last_timestamp}-{self.last_sequence}"
                ),
            )
            return self.encoder.encode_bulk_string(
                f"{self.last_timestamp}-{self.last_sequence}"
            )

        return validation

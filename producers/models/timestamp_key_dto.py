from dataclasses import dataclass

# Define a Data Transfer Objects so changes to the schema only need to be carried over to one place.
@dataclass(frozen=True)
class TimestampKeyDto:
    timestamp: int
"""Creates a turnstile data producer"""
import logging
from dataclasses import dataclass, asdict
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware
from models.timestamp_key_dto import TimestampKeyDto

logger = logging.getLogger(__name__)

# Define a Data Transfer Objects so changes to the schema only need to be carried over to one place.
@dataclass(frozen=True)
class TurnstileEntryDto:
    station_id: int
    station_name: str
    line: str

class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load( f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # Use a versioning so we do not have to setup the docker container once we run into schema conflicts
        # TODO: Remove in final version
        topic_name = f'station.{station_name}.turnstile.entry.v1'
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp_for_mock, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp_for_mock, time_step)

        for i in range(num_entries):

            valueDto = TurnstileEntryDto(
                station_id=self.station.station_id,
                station_name=self.station.name,
                line=self.station.color.name
            )
            keyDto = TimestampKeyDto(self.time_millis())
            self.producer.produce(
                topic=self.topic_name,
                key=asdict(keyDto),
                value=asdict(valueDto),
                value_schema=self.value_schema,
                key_schema=self.key_schema
            )

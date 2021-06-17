"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


CONNECT_TOPIC_NAME = 'cta.information.stations'
TRANSFORMED_TOPIC_NAME = f'{CONNECT_TOPIC_NAME}.transformed.v1'

app = faust.App(
    "stations-stream",
    broker="kafka://localhost:9092",
    store="memory://"
)

station_information_topic = app.topic(
    CONNECT_TOPIC_NAME,
    value_type=Station
)

transformed_topic = app.topic(
    TRANSFORMED_TOPIC_NAME,
    partitions=1,
    value_type=TransformedStation
)

table = app.Table(
    'stations_table',
    default=TransformedStation,
    partitions=1,
    changelog_topic=transformed_topic
)

def lineFromStation(station: Station) -> str:
    if station.red:
        return 'red'

    if station.blue:
        return 'blue'

    if station.green:
        return 'green'

    return '-'

@app.agent(station_information_topic)
async def stationInformation(stations):
    async for station in stations:

        # Determine the line
        line = lineFromStation(station)

        # Transform the information
        transformedStation = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )

        # Store in the table
        table[station.station_id] = transformedStation


if __name__ == "__main__":
    app.main()

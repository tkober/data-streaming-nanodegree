"""Methods pertaining to weather data"""
from dataclasses import dataclass, asdict
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)

# Define a Data Transfer Objects so changes to the schema only need to be carried over to one place.
@dataclass(frozen=True)
class WeatherUpdateDto:
    temperature: float
    status: str

@dataclass(frozen=True)
class WeatherUpdateKeyDto:
    timestamp: int

class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        # Use a versioning so we do not have to setup the docker container once we run into schema conflicts
        # TODO: Remove in final version
        super().__init__(
            "weather.update.v1",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions= 1,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        keyDto = WeatherUpdateKeyDto(self.time_millis())
        valueDto = WeatherUpdateDto(temperature=self.temp, status=self.status.name)

        headers = { 'Content-Type': 'application/vnd.kafka.avro.v2+json' }
        data = {
            'value_schema': json.dumps(Weather.value_schema),
            'key_schema': json.dumps(Weather.key_schema),
            'records': [{
                'value': asdict(valueDto),
                'key': asdict(keyDto)
            }]
        }
        url = f'{Weather.rest_proxy_url}/topics/{self.topic_name}'
        response = requests.post(url=url, data=json.dumps(data), headers=headers)
        try:
            response.raise_for_status()
        except:
            logger.error(f'Failed to send weather update to Kafka REST Proxy: status -> { response.status_code }\n{ json.dumps(response.json(), indent=2) }')
            exit(-1)

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )

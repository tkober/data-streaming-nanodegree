"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)

WEATHER_UPDATE_TOPIC = 'org.chicago.cta.weather.update.v1'

class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        if WEATHER_UPDATE_TOPIC == message.topic():
            self.temperature = message.value['temperature']
            self.status = message.value['status']

"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URLS = 'PLAINTEXT://localhost:9092'
# Use this if if you added the other two brokers in the docker-compose file
#BROKER_URLS = 'PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
CLIENT_TIMEOUT = 3

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            'bootstrap.servers': BROKER_URLS,
            'group.id': 'group-0'
        }

        if self.offset_earliest:
            self.broker_properties['auto.offset.reset'] = 'earliest'

        if is_avro is True:
            self.broker_properties['schema.registry.url'] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(config=self.broker_properties)
        else:
            self.consumer = Consumer(config=self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        try:
            message = self.consumer.poll(self.consume_timeout)

            if message is None:
                return 0

            if message.error() is not None:
                logger.error(f'Error occurred on consumer for topic "{self.topic_name_pattern}":\n{message.error()}')
                return 0

            self.message_handler(message)
            return 1

        except (RuntimeError, SerializerError) as e:
            logger.fatal(f'Fatal error occurred during poll for topic "{self.topic_name_pattern}":\n{e}')
            exit(-1)


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info(f'Closed consumer for topic "{self.topic_name_pattern}"')

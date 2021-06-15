"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URLS = 'PLAINTEXT://localhost:9092'
# Use this if if you added the other two brokers in the docker-compose file
#BROKER_URLS = 'PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
CLIENT_TIMEOUT = 3

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name: str,
        key_schema: str,
        value_schema: str = None,
        num_partitions: int = 1,
        num_replicas: int = 1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.kafka_client = AdminClient({'bootstrap.servers': BROKER_URLS})

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': BROKER_URLS,
            'schema.registry.url': SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # Use the CLIENT_TIMEOUT so that the client does not wait infinite if one of the brokers is not available.
        # Here it is mostly due to missing 2 brokers in the docker-compose file.
        if self.kafka_client.list_topics(timeout=CLIENT_TIMEOUT).topics.get(self.topic_name) is not None:
            logger.info(f'Existing topic "{self.topic_name}" found.')
        else:
            logger.warning(f'Topic "{self.topic_name}" not present yet.')

            newTopic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
                config={
                    'cleanup.policy': 'compact',
                    'compression.type': 'lz4',
                    'delete.retention.ms': 2000,
                    'file.delete.delay.ms': 30000
                }
            )
            futures = self.kafka_client.create_topics([newTopic])
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.warning(f'Created topic "{topic}".')
                except Exception as e:
                    logger.error(f'Failed to create topic "{topic}": {e}')
                    raise

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info(f'Flushed producer for topic {self.topic_name}')

    def time_millis(self) -> int:
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

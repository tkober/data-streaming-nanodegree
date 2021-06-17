"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "connection.url": "jdbc:postgresql://postgres:5432/cta",
        "connection.user": "cta_admin",
        "connection.password": "chicago",
        "table.whitelist": "stations",
        "mode": "incrementing",
        "incrementing.column.name": "stop_id",
        "topic.prefix": "org.chicago.cta.",
        # Poll once a day since the stations table does not hold volatile information.
        "poll.interval.ms": str(1000 * 60 * 60 * 24),
        "tasks.max": 1
    }

    response = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": config
        }),
    )

    try:
        response.raise_for_status()
    except:
        logger.error(
            f'Failed to create connector "{CONNECTOR_NAME}": status -> {response.status_code}\n{json.dumps(response.json(), indent=2)}')
        exit(-1)

    logger.info(f'Successfully created connector "{CONNECTOR_NAME}')


if __name__ == "__main__":
    configure_connector()

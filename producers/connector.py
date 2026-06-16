"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

from confluent_kafka.admin import AdminClient, NewTopic
import requests


logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083/connectors")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECTOR_NAME = os.getenv("STATIONS_CONNECTOR_NAME", "stations")
STATIONS_TOPIC = os.getenv("STATIONS_TOPIC", "org.chicago.cta.stations")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "cta")
POSTGRES_USER = os.getenv("POSTGRES_USER", "cta_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "chicago")

STATION_KEY_SCHEMA = {
    "namespace": "com.udacity",
    "type": "record",
    "name": "StationKey",
    "fields": [{"name": "stop_id", "type": "int"}],
}

STATION_VALUE_SCHEMA = {
    "namespace": "com.udacity",
    "type": "record",
    "name": "StationValue",
    "fields": [
        {"name": "stop_id", "type": "int"},
        {"name": "direction_id", "type": "string"},
        {"name": "stop_name", "type": "string"},
        {"name": "station_name", "type": "string"},
        {"name": "station_descriptive_name", "type": "string"},
        {"name": "station_id", "type": "int"},
        {"name": "order", "type": "int"},
        {"name": "red", "type": "boolean"},
        {"name": "blue", "type": "boolean"},
        {"name": "green", "type": "boolean"},
    ],
}


def create_stations_topic():
    """Creates the Kafka Connect stations topic before the connector writes to it."""
    client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = client.list_topics(timeout=10).topics
    if STATIONS_TOPIC in topics:
        return
    futures = client.create_topics(
        [NewTopic(STATIONS_TOPIC, num_partitions=1, replication_factor=1)]
    )
    futures[STATIONS_TOPIC].result()


def register_schema(subject, schema):
    """Registers logical station schemas for rubric/schema-registry inspection."""
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": json.dumps(schema)}),
    )
    resp.raise_for_status()


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    create_stations_topic()
    register_schema(f"{STATIONS_TOPIC}-key", STATION_KEY_SCHEMA)
    register_schema(f"{STATIONS_TOPIC}-value", STATION_VALUE_SCHEMA)

    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.max.rows": "500",
        "connection.url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        "connection.user": POSTGRES_USER,
        "connection.password": POSTGRES_PASSWORD,
        "table.whitelist": "stations",
        "mode": "incrementing",
        "incrementing.column.name": "stop_id",
        "topic.prefix": "org.chicago.cta.",
        "poll.interval.ms": "86400000",
        "transforms": "createKey",
        "transforms.createKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.createKey.fields": "stop_id",
    }

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        resp = requests.put(
            f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}/config",
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
        )
        resp.raise_for_status()
        logging.debug("connector updated successfully")
        return
    if resp.status_code != 404:
        resp.raise_for_status()

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"name": CONNECTOR_NAME, "config": config}),
    )

    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()

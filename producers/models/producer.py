"""Producer base-class providing common utilites and functionality"""
import logging
import os
import time


from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    admin_client = None
    producers = {}

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "schema.registry.url": os.getenv(
                "SCHEMA_REGISTRY_URL", "http://localhost:8081"
            ),
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        if self.key_schema is not None or self.value_schema is not None:
            self.producer = self.get_producer()
        else:
            self.producer = None

    @classmethod
    def schema_cache_key(cls, key_schema, value_schema):
        """Returns a stable cache key for producers that share schemas."""
        return (str(key_schema), str(value_schema))

    def get_producer(self):
        """Reuses AvroProducer instances so each station does not open a socket."""
        cache_key = Producer.schema_cache_key(self.key_schema, self.value_schema)
        if cache_key not in Producer.producers:
            Producer.producers[cache_key] = AvroProducer(
                self.broker_properties,
                default_key_schema=self.key_schema,
                default_value_schema=self.value_schema,
            )
        return Producer.producers[cache_key]

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if Producer.admin_client is None:
            Producer.admin_client = AdminClient(
                {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
            )
        client = Producer.admin_client
        topic_metadata = client.list_topics(timeout=10)
        if self.topic_name in topic_metadata.topics:
            logger.debug("topic already exists: %s", self.topic_name)
            return

        topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
        )
        futures = client.create_topics([topic])
        try:
            futures[self.topic_name].result()
            logger.info("created topic %s", self.topic_name)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            logger.debug("topic already exists: %s", self.topic_name)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        for producer in Producer.producers.values():
            producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

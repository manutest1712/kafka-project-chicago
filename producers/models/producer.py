"""Producer base-class providing common utilities and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

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

        #
        # Kafka + Schema Registry properties
        #
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }

        # Create topic if needed
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        #
        # AvroProducer configuration
        #
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties["bootstrap.servers"],
                "schema.registry.url": self.broker_properties["schema.registry.url"],
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info(f"Checking/creating topic: {self.topic_name}")

        admin_client = AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )

        # Get existing topics from cluster
        topic_metadata = admin_client.list_topics(timeout=10)

        if self.topic_name in topic_metadata.topics:
            logger.info(f"Topic '{self.topic_name}' already exists, skipping creation.")
            return

        logger.info(f"Topic: '{self.topic_name}' does not exist")

        # Create new topic
        new_topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
        )

        fs = admin_client.create_topics([new_topic])

        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic '{topic}' successfully created.")
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {e}")

    def time_millis(self):
        """Use this function to get the key for Kafka events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        if not hasattr(self, "producer") or self.producer is None:
            logger.warning("Producer was not initialized. Nothing to close.")
            return
        
        try:
            self.producer.flush(10)
            logger.info("Producer flush complete.")
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")

        logger.info("Producer closed.")

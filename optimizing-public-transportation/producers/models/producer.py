"""Producer base-class providing common utilites and functionality"""
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

        # Todo move this for environment variables
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://kafka0:9092",
            "schema.registry.url": "http://schema-registry:8081",
        }
        self.client = AdminClient({"bootstrap.servers": "PLAINTEXT://kafka0:9092"})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )
        logger.info("Producer has been initialized...")

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        topic_metadata = self.client.list_topics(timeout=5)
        topics = topic_metadata.topics
        if self.topic_name in topics:
            logger.info(f"Topic already exist {self.topic_name}")
            return

        t = self.client.create_topics([
            NewTopic(self.topic_name, num_partitions=1, replication_factor=1)
        ])
        print(t)        
        logger.info(f"Topic {self.topic_name} created !")        
    
    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("Producer flushed successfully")
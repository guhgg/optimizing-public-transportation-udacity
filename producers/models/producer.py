import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8086"

class Producer:

    existing_topics = set([])

    def __init__(self, topic_name, key_schema, value_schema=None, num_partitions=1, num_replicas=1):

        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties,
                                     self.default_key_schema=key_schema,
                                     self.default_value_schema=value_schema)

    def create_topic(self):

        if self.topic_exists(self.topic_name):
            return

        admClient = AdminClient({"bootstrap.servers": BROKER_URL})
        future = admClient.create_topics([NewTopic(
            topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])

        for _, future in futures.items():
            try:
                future.result()
                logger.error(f"{self.topic_name} created {e}")
            except Exception as e:
                logger.error(f"{self.topic_name} not created: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        self.producer.flush(timeout=10)

    def time_millis(self):
        return int(round(time.time() * 1000))

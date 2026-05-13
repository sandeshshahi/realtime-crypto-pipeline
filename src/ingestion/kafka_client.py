import json
import os
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Path to the Avro schema file (the data contract)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'trade_value.avsc')


def _load_avro_schema() -> str:
    """Reads the .avsc file and returns the schema string."""
    with open(SCHEMA_PATH, 'r') as f:
        return f.read()


def _trade_to_dict(trade, ctx):
    """
    Converts a trade dictionary into the format expected by the Avro schema.
    This callback is used by the AvroSerializer.
    """
    return {
        "e": str(trade["e"]),
        "E": int(trade["E"]),
        "s": str(trade["s"]),
        "p": str(trade["p"]),
        "q": str(trade["q"]),
        "T": int(trade["T"]),
    }


class CryptoKafkaPublisher:
    """
    A decoupled client responsible solely for publishing messages to Kafka.
    Now uses Avro serialization with Confluent Schema Registry for data contracts.
    """
    def __init__(self):
        broker = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('KAFKA_TOPIC')
        schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL')

        if not broker or not self.topic or not schema_registry_url:
            logger.error("CRITICAL: Missing Kafka or Schema Registry environment variables.")
            exit(1)

        try:
            # Connect to Schema Registry
            schema_registry_conf = {'url': schema_registry_url}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            # Load the Avro schema (the data contract) and create the serializer
            avro_schema_str = _load_avro_schema()
            self.avro_serializer = AvroSerializer(
                schema_registry_client,
                avro_schema_str,
                _trade_to_dict
            )
            self.string_serializer = StringSerializer('utf_8')

            # Create the Confluent Kafka producer
            producer_conf = {'bootstrap.servers': broker}
            self.producer = Producer(producer_conf)

            logger.info(f"Connected to Kafka broker at {broker}")
            logger.info(f"Schema Registry connected at {schema_registry_url}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            exit(1)

    def publish(self, data: dict):
        """
        Serializes data with Avro (validating against the schema contract)
        and sends it to the configured Kafka topic.
        """
        try:
            self.producer.produce(
                topic=self.topic,
                key=self.string_serializer(data.get('s', 'unknown')),
                value=self.avro_serializer(data, SerializationContext(self.topic, MessageField.VALUE)),
                on_delivery=self._delivery_report
            )
            # Trigger delivery callbacks without blocking
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")

    def _delivery_report(self, err, msg):
        """Called once for each produced message to indicate delivery result."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")

    def close(self):
        """Flushes remaining messages and closes the connection."""
        self.producer.flush()
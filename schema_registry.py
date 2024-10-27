# 3rd party library imported
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
import logging
# imort from constants
from constants import SCHEMA_STR

schema_registry_url = 'http://localhost:8081'
kafka_topic = 'craft-demo-topic'
schema_registry_subject = f"{kafka_topic}-value"

class SchemaRegistryUtils:
    def __init__(self, schema_registry_url, kafka_topic, schema_registry_subject):
        self.schema_registry_url = schema_registry_url
        self.kafka_topic = kafka_topic
        self.schema_registry_subject = schema_registry_subject

    def get_schema_from_schema_registry(self):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        latest_version = sr.get_latest_version(self.schema_registry_subject)

        return sr, latest_version

    def register_schema(self, schema_str):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = sr.register_schema(subject_name=self.schema_registry_subject, schema=schema)

        return schema_id

    def update_schema(self, schema_str):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        versions_deleted_list = sr.delete_subject(self.schema_registry_subject)
        logging.info(f"versions of schema deleted list: {versions_deleted_list}")

        schema_id = register_schema(self.schema_registry_url, schema_registry_subject, schema_str)
        return schema_id

schema_id = register_schema(schema_registry_url, schema_registry_subject, SCHEMA_STR)
logging.info(schema_id)

sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
logging.info(latest_version.schema.schema_str)

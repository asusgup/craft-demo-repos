import requests
import json
from time import sleep
from jproperties import Properties
from MySQL_connect import build_Mysql_Connection
from dynamoDB_connect import insertInto_DynamoDB
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
from schema_registry import SchemaRegistryUtils
import logging


mysql_configs = Properties()
with open('mysql-config.properties', 'rb') as config_file:
		 mysql_configs.load(config_file)

kafka_configs = Properties()
with open('kafka-configs.properties', 'rb') as config_file:
		 kafka_configs.load(config_file)

source_url = 'https://app.intuit.events/cdc?entity=cmpny_data&updatedSince={last_upd_dt}'

kafka_url = kafka_configs.get("BOOTSTRAP_SERVER").data
schema_registry_url = kafka_configs.get("SCHEMA_REGISTRY_URL").data
kafka_topic = kafka_configs.get("KAFKA_TOPIC").data
schema_registry_subject = f"{kafka_topic}-value"
Security_protocol = kafka_configs.get("SECURITY_PROTOCOL").data
delivery_timeout = kafka_configs.get("DELIVERY_TIMEOUT_MS").data
idempotence = kafka_configs.get("ENABLE_IDEMPOTENCE").data
delivery_timeout = kafka_configs.get("DELIVERY_TIMEOUT_MS").data
max_inflight_requests = kafka_configs.get("MAX_IN_FLIGHT_REQUESTS").data
linger_ms = kafka_configs.get("LINGER_MS").data


def delivery_report(errmsg, msg):
    if errmsg is not None:
        logging.info("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    logging.info('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))


def publish_to_Kafka(topic_name, decoded_json, producer):
    try:
        producer.produce(topic=topic_name, key=decoded_json["key"], value=decoded_json["value"], on_delivery=delivery_report)

        # Trigger any available delivery report callbacks from previous produce() calls
	logging.info("Trigger any available delivery report callbacks from previous produce() calls")    
        events_processed = producer.poll(1)
        logging.info(f"events_processed: {events_processed}")

        messages_in_queue = producer.flush(1)
        logging.info(f"messages_in_queue: {messages_in_queue}")
    except Exception as e:
        logging.error(e)
        insertInto_DynamoDB(decoded_json)    


def avro_producer(source_url, kafka_url, schema_registry_url, schema_registry_subject):
    # schema registry
    logging.info("Initialising schema registry")	
    registryObj = SchemaRegistryUtils(schema_registry_url, kafka_topic, schema_registry_subject)

    sr, latest_version = registryObj.get_schema_from_schema_registry()

    value_avro_serializer = AvroSerializer(schema_registry_client = sr,
                                          schema_str = latest_version.schema.schema_str,
                                          conf={
                                              'auto.register.schemas': False,
                                              'use.latest.version': True
                                            }
                                          )

    # Kafka Producer
    logging.info("Initialising kafka producer")	
    producer = SerializingProducer({
        'bootstrap.servers': kafka_url,
        'security.protocol': Security_protocol,
        'value.serializer': value_avro_serializer,
        'key.serializer': lambda x: json.dumps(x).encode('utf-8'),
        'delivery.timeout.ms': delivery_timeout,
        'enable.idempotence': idempotence,
        'delivery.timeout.ms': delivery_timeout,
        'max.in.flight.requests.per.connection': max_inflight_requests,
        'linger.ms': linger_ms
    })

    logging.info("Getting latest upd timestamp from mysql rds")
    connection, cursor = build_Mysql_Connection()
    cursor.execute(f"Select max(last_upd) from {db}.cmpny_data where feed_name = 'cmpny_data'".format(db=mysql_configs.get("mysql_report_database").data))
    last_upd_dt = cursor.fetchall()[0][0]
    source_url_final = source_url.format(last_upd_dt = last_upd_dt)

    s = requests.Session()
    CURRENT_MAX_UPD_DT = 0
    logging.info("Calling API for its GET response")	
    with s.get(source_url_final, headers=None, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                decoded_line = line.decode()
                if decoded_line.find("key") >= 0 and decoded_line.find("value") >= 0:
                    # convert to json
                    CURRENT_MAX_UPD_DT = max(CURRENT_MAX_UPD_DT, decoded_json["value"]["ts_ms"])
                    decoded_json = json.loads(decoded_line)
		    logging.info("publishing data to Kafka")	
                    publish_to_Kafka(kafka_topic, decoded_json, producer)
                    
                else:
                    # PUBLISING THE BAD DATA TO DynamoDB
		    logging.info("PUBLISING THE BAD DATA TO DynamoDB")	
                    insertInto_DynamoDB(decoded_json) 

    logging.info("Updating latest upd timestamp for CURRENT_MAX_UPD_DT to mysql rds")
    connection, cursor = build_Mysql_Connection()
    cursor.execute(f"UPDATE {db}.cmpny_data SET last_upd = CURRENT_MAX_UPD_DT WHERE feed_name = 'cmpny_data'")                    


while True:
    avro_producer(source_url, kafka_url, schema_registry_url, schema_registry_subject)
    logging.info("Putting a delay of 3seconds b/w 2 GET reuest to API")	
    sleep(3000)

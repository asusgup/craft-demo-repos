from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError
from pyspark.sql import SparkSession
from jproperties import Properties
from pyspark.sql.functions import col
from delta.tables import *
from datetime import datetime
import logging

kafka_configs = Properties()
with open('kafka-configs.properties', 'rb') as config_file:
		 kafka_configs.load(config_file)

spark = SparkSession.builder.appName("craft_demo").master("local[1]").getOrCreate()

def flatten_json_and_Write_To_S3(after_json, primary_keys):

    # Reading and flatteing the data
    # Took example of following sample json event => https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html#Exam

    logging.info("Reading and flatteing the data")	
    df = spark.read.option("multiline", "true").option("escape", "\"").json(after_json)
    flatten_df = df.withColumn("batters_ids",col("batters.batter.id"))\
      .withColumn("batters_types",col("batters.batter.type"))\
      .withColumn("topping_ids",col("topping.id"))\
      .withColumn("topping_types",col("topping.type"))\
      .drop("batters","topping")

    s3_bucket = kafka_configs.get('S3_RAW_BUCKET_PATH').data
    today_date = datetime.today().strftime("%Y-%m-%d")

    # Writing the flattened data as csv format in today's date partition filter
    logging.info("Writing the flattened data as csv format in today's date partition filter")
    flatten_df.write.format("csv").option("header","true").save(f"s3_bucket/{date}".format(date = today_date))


def consumer_Processing(data, primary_keys):
    # Here We are flattening the json data and merging with the target table in data lake

    data_without_delete_events = []
    logging.info("Filtering out the deleted events on source side, ")	
    for events in data:
        if events['op'] != 'D':
	    # Filtering out the deleted events on source side,
	    events['after']['op']=events['op']	
            data_without_delete_events.append(events['after'])
	else:
	    events['before']['op']=events['op']	
            data_without_delete_events.append(events['before'])	

    flatten_json_and_Write_To_S3(data_without_delete_events, primary_keys)

# Defining Consumer Configs
consumer_config = {
        "bootstrap.servers": kafka_configs.get("BOOTSTRAP_SERVER").data,
        "schema.registry.url": kafka_configs.get("SCHEMA_REGISTRY_URL").data,
        "use.latest.version": True,
        "group.id": 1,
        "auto.offset.reset": kafka_configs.get("AUTO_OFFSET_RESET").data,
        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer": "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        'enable.auto.commit': False
}

consumer = AvroConsumer(consumer_config)
consumer.subscribe([kafka_configs.get("KAFKA_TOPIC").data])


try:
    # Poll for records
    logging.info("Polling for records from kafka")	
    while True:
        records = consumer.poll(timeout=1000)  # Adjust timeout as needed
        if records:
            data = []
            primary_keys=[]

            # iterating thru every event message
            for msg in records:
                # Handle errors
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event, can be ignored
			logging.error(msg.error())    
                        continue
                    else:
                        logging.info(f"Error: {msg.error()}")
                        break

                # Process the message
                key = msg.key().decode('utf-8')  # Decode key from bytes to string
                value = msg.value()  # Deserialize the Avro value

                # Find out the primary keys and data from the json value
                primary_keys = json.loads(key).keys()
                data.append(value)

                logging.info(f"Consumed record with key: {key} and value: {value}")
        
            # Starting ETL processing of data
	    try:	
               consumer_Processing(data,primary_keys)
	    except Exception as e:
		    logging.error(f"Data processing at consumer end failed due to error - {err}".format(err=e))
		    continue   # Stopping at furthure Commit to kafka as message processing failed
        else:
            print("No records found.")

        # Commiting Offsets back to kafka    
	logging.info("Commiting Offsets back to kafka")
        consumer.commit(asynchronous=False)    

finally:
    consumer.close()    

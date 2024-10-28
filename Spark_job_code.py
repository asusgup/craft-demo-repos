from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import *
from jproperties import Properties
from datetime import datetime, timedelta
import logging

kafka_configs = Properties()
with open('kafka-configs.properties', 'rb') as config_file:
		 kafka_configs.load(config_file)

# Creating Spark Session
logging.info("Creating Spark Session")
spark = SparkSession.builder.appName("craft_demo").config("spark.sql.extensions", "org.apache.spark.sql.delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("yarn").getOrCreate()

s3_bucket = kafka_configs.get('S3_RAW_BUCKET_PATH').data
primary_keys = kafka_configs.get('PRIMARY_KEYS').data

# Reading S3 data with yesterday date 
logging.info("Reading S3 data with yesterday date")
prev_date = datetime.today().strftime("%Y-%m-%d") - timedelta(days=1)
cdc_events_source = spark.read.option("inferSchema","true").option("header","true").csv(f"s3_bucket/{date}".format(date = prev_date))

# Target Table
targetTable = DeltaTable.forName(spark, "target_table")

# Computing the merge condition to be applied on delta lake
logging.info("Computing the merge condition to be applied on delta lake")
merge_condition = ""
for idx in range(len(primary_keys)):
    key = primary_keys[idx]
    merge_condition+= f"old_data.{0} = new_events.{0} ".format(keys)
    if idx < len(primary_keys)-1:
        merge_condition+= "and "

merge_condition = merge_condition.strip()

# Applying the merge statement at Target Delta table to perform UPSERTS and remove DELETED data from source
logging.info("Applying the merge statement at Target Delta table to perform UPSERTS and remove DELETED data from source")
targetTable.alias("old_data")\
.merge(
    cdc_events_source.alias("new_events"), merge_condition
)\
.whenMatchedUpdateAll("new_events.op <> 'D'")\
.whenNotMatchedInsertAll("new_events.op <> 'D'")\
.whenMatchedDeleteAll("new_events.op == 'D'")\
.execute()

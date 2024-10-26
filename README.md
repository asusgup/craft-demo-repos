# craft-demo-repos



![intuit_craft_demo7](https://github.com/user-attachments/assets/949ef7e3-c9b3-424c-a238-f61e6a0e7fa5)

# Key points on CDC event processing.

* The target table is assumed to be in delta format, This is considered to use delta lake’s ACID semantics for efficient UPSERTS.
* The API GET Response is assumed not to contain any table schema/DDL changes. Only events like I, U, D  (Insert, update , delete) records are included for simplicity and events contain all details of the columns required on target side.
* The API is exposed internally within the system. It is not exposed publicly So we are assuming here that the APIs need no data sanity check and will be used mostly as-expected. Also this eliminates the need of authentication and authorization. 
* The API endpoint is highly available. So there is no lag in polling the events at a specific frequency.
* The API response is assumed to be of valid json schema. 
* In case of Kafka Service being unavailable, The dynamoDB events are given priority than the producer SDK events to be pushed to  kafka to maintain message ordering.
* The source and target tables are normalized, assumed to have a primary key, So that we can merge those events to Target table on the basis of that primary key.
* A Retry producer Lambda Function is configured to retry sending the messages from DynamoDB., if retriable to Kafka topics once it is available. 
* Ambiguous messages in DynamoDB are further analyzed by a Retry Lambda Function utility. if they can be retried sending to kafka.
* A Retry producer Lambda Function is configured to retry sending the messages from DynamoDB., if retriable to Kafka topics once it is available.
* Consumer Applications consume messages from kafka topics, and stores the raw, flattened json data in Raw S3 Bucket.
* The Spark Batch Job can read the data from S3 raw bucket, transform the data and write into target table in delta lake format.
* The Spark Stream Job can read data from S3 raw bucket in real-time, transform and write into Target table in delta lake format.
* If the ambiguous messages  in DynamoDB are non-retriable, then those events can be considered as stale events and appropriate notification/alerts can be triggered to the concerned teams. These messages can then be deleted from DynamoDB.


# Managed services used in the design

* AWS Managed Kafka - Serves as a distributed, scalable and fault-tolerant messaging system. It could help scale to billions of events messages.

* Schema Registry - Serves as a centralized schema management and storage, which makes it easier to track schema changes of source table.

* AWS Lambda - Serves as a event based serverless function utility which checks for bad/stale records in DynamoDB and retries the message publishing back to kafka. Also responsible for triggering critical alert msgs to concerned teams.

* AWS DynamoDB - DynamoDB supports eventual consistency and high availability so that during kafka service unavailability, The events could be efficiently stored in DynamoDB. 

# craft-demo-repos

![intuit_craft_demo4](https://github.com/user-attachments/assets/a31f8bcd-1d9d-4762-bb70-dc4bc1c6f6c7)



* Producer SDK fetches the changes events from the source system via GET request (GET https://<domain>/cdc?entity=<table_name>&updatedSince=<last_fetched_timestamp>) and stores the last event timestamp in RDS.

* Initially, the fetched event schemas are registered in schema registry.

* The kafka source application publishes the events into Kafka topic using producer SDK.

* When an application starts up, an event schema is cached in the producing application for high performance.

* The SDK performs schema validation to ensure the event is as per the schema.

* If the Validation passed, producer send the event to kafka topic(with Avro serialization). In case of validation error, Events are routed to DynamoDB as ambiguous messages for further analysis.

* If Kafka server is unavailable, then the messages are written to DynamoDB (To have resilient and fault tolerant system).
* A Retry producer Lambda Function is configured to retry sending the messages from DynamoDB., if retriable to Kafka topics once it is available. 
* Ambiguous messages in DynamoDB are further analyzed by a Retry Lambda Function utility. if they can be retried sending to kafka.
* A Retry producer Lambda Function is configured to retry sending the messages from DynamoDB., if retriable to Kafka topics once it is available.
* Consumer Applications consume messages from kafka topics, and stores the raw, flattened json data in Raw S3 Bucket.
* The Spark Batch Job can read the data from S3 raw bucket, transform the data and write into target table in delta lake format.
* The Spark Stream Job can read data from S3 raw bucket in real-time, transform and write into Target table in delta lake format.
* If the ambiguous messages  in DynamoDB are non-retriable, then those events can be considered as stale events and appropriate notification/alerts can be triggered to the concerned teams. These messages can then be deleted from DynamoDB.


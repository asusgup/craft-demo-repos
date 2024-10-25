# craft-demo-repos

![intuit_craft_demo4](https://github.com/user-attachments/assets/a31f8bcd-1d9d-4762-bb70-dc4bc1c6f6c7)



* Producer SDK fetches the changes events from the source system via GET request (GET https://<domain>/cdc?entity=<table_name>&updatedSince=<last_fetched_timestamp>) and stores the last event timestamp in RDS.

* Initially, the fetched event schemas are registered in schema registry.

* The kafka source application publishes the events into Kafka topic using producer SDK.

* When an application starts up, an event schema is cached in the producing application for high performance.

* The SDK performs schema validation to ensure the event is as per the schema.

* If the Validation passed, producer send the event to kafka topic(with Avro serialization). In case of validation error, Events are routed to DynamoDB as ambiguous messages for further analysis.

* If Kafka server is unavailable, then the messages are written to DynamoDB (To have resilient and fault tolerant system). 

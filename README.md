# Dialogflow Log Parser

This repository contains an example of how to leverage Cloud Dataflow and BigQuery to view Dialogflow interactions.

The Pipeline Steps are as follows:

1. Dialogflow Interactions are logged to Google Cloud Logging
2. A Cloud Logging sink sends the log messages to Cloud Pub/Sub
3. Dataflow process the __textpayload__ and streams it to BigQuery
4. Access to the log interactions are now available in BigQuery

![Dialogflow Log Parser Diagram](images/diagram.png)

__Note:__ Dialogflow Interactions Logging is sent to Cloud Logging as a Text Payload, this code will parse the Text Payload to a structured format within BigQuery which is defined in the Dataflow code.

## BigQuery Schema
You can modify the schema.json file to define the BigQuery's table schema.

## Create the table using schema json file:

If you're creating an empty table, use the bq mk command.

```
bq mk --table project_id:dataset.table path_to_schema_file
```
## Installation

1. Enable the Dataflow API
    ```sh
    gcloud services enable dataflow
    ```

2. Create a Storage Bucket for Dataflow Staging

    ```sh
    gsutil mb gs://[BUCKET_NAME]/
    ```

3. Create a folder in the newly created bucket in the Google Cloud Console Storage Browser called __tmp__

4. Create a Pub/Sub Topic
    ```sh
    gcloud pubsub topics create [TOPIC_NAME]
    ``` 
5. Create a Cloud Logging sink
    ```sh
    gcloud logging sinks create [SINK_NAME] pubsub.googleapis.com/projects/[PROJECT_ID]/topics/[TOPIC_NAME] --log-filter="resource.type=global"
    ```

6. Install the Apache Beam GCP Library
    ```sh
    python3 -m virtualenv tempenv
    source tempenv/bin/activate
    pip install apache-beam[gcp]
    ```

7. Create BigQuery dataset

8. Deploy Dataflow Job
    ```sh
    python3 stackdriverdataflowbigquery.py --project=[YOUR_PROJECT_ID] \ 
    --input_topic=projects/[YOUR_PROJECT_ID]/topics/[YOUR_TOPIC_NAME] \ 
    --runner=DataflowRunner --temp_location=gs://[YOUR_DATAFLOW_STAGING_BUCKET]/tmp \
    --output_bigquery=[YOUR_BIGQUERY_DATASET.YOUR BIGQUERY_TABLE] --region=us-central1 \
    --bigquery_schema=[YOUR_BIGQUERY_SCHEMA_JSON_FILE]
    ```

9. Enable Dialogflow Logs to Cloud Logging

    Enable Log interactions to Dialogflow and Google Cloud
    https://cloud.google.com/dialogflow/docs/history#access_all_logs

Once you enable Enable Log interactions, your new Dialogflow interactions will be available in BigQuery

**This is not an officially supported Google product**
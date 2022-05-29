# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START parsing Stackdriver logs from pubsub_to_bigquery]
import argparse
import logging
import json
import re
import ast

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

# function that looks for a key in a dictionary and returns the value.
def get_value_from_multidimensional_dict(key, dictionary):  
    for k, v in dictionary.items():
        if k == key:
            return v
        elif isinstance(v, dict):
            value = get_value_from_multidimensional_dict(key, v)
            if value:
                return value
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                  value = get_value_from_multidimensional_dict(key, item)
                  if value:
                      return value
                
    return None

# function that takes a list of keys and a dictionary and returns a dictionary with the values of the keys
def get_values_from_multidimensional_dict(keys, dictionary):
    return_dict = {}
    for key in keys:
        logging.info('Processing key: ' + key)
        value = get_value_from_multidimensional_dict(key, dictionary)
        if isinstance(value, str):
            return_dict[key] = value
        elif isinstance(value, dict):
            logging.info('Processing dict: ' + str(value))
            return_dict[key] = json.dumps(value)
        elif isinstance(value, list):
            logging.info('Processing list: ' + str(value))
            return_dict[key] = json.dumps(value)
    return return_dict


# function to get response body data from pub/sub message and build structure for BigQuery load
def parse_transform_response(data):
    output = ['error_type', 'session_id', 'intentDetectionConfidence', 'agent_id', 'responseId', 'receiveTimestamp', 'error_type', 'string_value', 'jsonPayload', 'parameters', 'currentPage', 'timestamp', 'logName', 'insertId', 'trace','webhook_for_slot_filling_used', 'is_fallback_intent']
    logging.info('--- START parse_transform_response Function ---')
    pub_sub_data = json.loads(data)
    fullpayload_dict = get_values_from_multidimensional_dict(output,pub_sub_data)
    logging.info('--- End parse_transform_response Function ---')
    return fullpayload_dict

def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument('--output_bigquery', required=True,
                        help='Output BQ table to write results to '
                             '"PROJECT_ID:DATASET.TABLE"')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        messages = (p
                    | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                    .with_output_types(bytes))
    else:
        messages = (p
                    | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                    .with_output_types(bytes))

    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(lambda x: x.decode('utf-8'))

    # Parse response body data from pub/sub message and build structure for BigQuery load
    output = decode_messages | 'ParseTransformResponse' >> beam.Map(parse_transform_response)

    # Write to BigQuery
    bigquery_table_schema = {
        "fields": [                 
                    {
                        "mode": "NULLABLE",
                        "name": "session_id",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "intentDetectionConfidence",
                        "type": "FLOAT"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "agent_id",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "responseId",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "receiveTimestamp",
                        "type": "TIMESTAMP"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "error_type",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "string_value",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "jsonPayload",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "parameters",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "currentPage",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "timestamp",
                        "type": "TIMESTAMP"
                    }    
                ]
    }
    output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_bigquery,
            schema=bigquery_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    p.run()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
# [END parsing Stackdriver logs from pubsub_to_bigquery]
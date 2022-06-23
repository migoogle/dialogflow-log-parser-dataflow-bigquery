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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


# function to read a json file and create a dictionary
def read_json_file(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data

# function that gets a dictionary and returns a list with the values of an specific key
def get_values_from_dict(key, dictionary):
    return_list = []
    for k, v in dictionary.items():
        if k == key:
            return_list.append(v)
        elif isinstance(v, dict):
            value = get_values_from_dict(key, v)
            if value:
                return_list.append(value)
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    value = get_values_from_dict(key, item)
                    if value:
                        return_list.append(value[0])
    return return_list

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
        if isinstance(value, str) or isinstance(value, float):
            return_dict[key] = value
        elif isinstance(value, dict):
            # logging.info('Processing dict: ' + str(value))
            return_dict[key] = json.dumps(value)
        elif isinstance(value, list):
            # logging.info('Processing list: ' + str(value))
            return_dict[key] = json.dumps(value)
    return return_dict


# function to get response body data from pub/sub message and build structure for BigQuery load
def parse_transform_response(data):
    logging.info('--- START parse_transform_response Function ---')
    pub_sub_data = json.loads(data)
    fullpayload_dict = get_values_from_multidimensional_dict(
        filter, pub_sub_data)
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
    parser.add_argument('--bigquery_schema', required=True,
                        help='BigQuery schema to use for the output table')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
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

    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(
        lambda x: x.decode('utf-8'))

    # Parse response body data from pub/sub message and build structure for BigQuery load
    output = decode_messages | 'ParseTransformResponse' >> beam.Map(
        parse_transform_response)

    # Write to BigQuery
    output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        known_args.output_bigquery,
        schema=bigquery_table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    # Schema for BigQuery table:
    bigquery_table_schema_list = read_json_file('known_args.bigquery_schema')
    bigquery_table_schema = {'fields': bigquery_table_schema_list}
    filter = get_values_from_dict('name', bigquery_table_schema)
    run()
# [END parsing Stackdriver logs from pubsub_to_bigquery]

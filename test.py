import json

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
                        print(value[0])
                        return_list.append(value[0])
    return return_list


# function to read a json file and create a dictionary
def read_json_file(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data

# Schema for BigQuery table:
bigquery_table_schema_list = read_json_file('/Users/miguens/dialogflow-log-parser-dataflow-bigquery/schema.json')
bigquery_table_schema = {'fields': bigquery_table_schema_list}

list = get_values_from_dict('name', bigquery_table_schema)
print(list)

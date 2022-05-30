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


list = get_values_from_dict('name', bigquery_table_schema)
print(list)

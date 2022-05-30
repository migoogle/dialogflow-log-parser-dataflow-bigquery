# function that takes a key, and look for it in a multidimentional dictionary and returns the value
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
                value = get_value_from_multidimensional_dict(key, item)
                if value:
                    return value
    return None

# function that takes a list of keys and a dictionary and returns a dictionary with the values of the keys
def get_values_from_multidimensional_dict(keys, dictionary):
    return_dict = {}
    for key in keys:
        value = get_value_from_multidimensional_dict(key, dictionary)
        if value:
            return_dict[key] = value
    return return_dict


# function that iterates through a multidimensional dictionary and generates a single dictionary
def iterate_multidimensional_single_dict(dictionary):
    return_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            return_dict.update(iterate_multidimensional(value))
        else:
            return_dict[key] = value
    return return_dict

# creating a dictionary with the shared leaves between two input dictionaries.
def shared(dictionary_1, dictionary_2):
    return_dict = {}
    for key, value in dictionary_1.items():
        if key in dictionary_2:
            if isinstance(value, dict):
                return_dict[key] = shared(value, dictionary_2[key])
            else:
                return_dict[key] = value
    return return_dict

# function that creates a dictionary with the shared leaves between a list of keys and a dictionary
def iterate_multidimensional(dictionary, keys=None):
    return_dict = {}
    if keys is None:
        keys = []
    for key, value in dictionary.items():
        if isinstance(value, dict):
            return_dict.update(iterate_multidimensional(value, keys + [key]))
        else:
            return_dict[key] = value
    return return_dict
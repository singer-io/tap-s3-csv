import singer

LOGGER = singer.get_logger()

def infer(key,datum,date_overrides):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        if type(datum) is dict:
            return 'dict'
        elif type(datum) is list:
            if not datum:
                return "list"
            else:
                return "list." + infer(key,datum[0],date_overrides)
        if key in date_overrides:
            return "date-time"
        elif type(datum) is int:
            return 'integer'
        elif type(datum) is float:
            return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'


def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        date_overrides = table_spec.get('date_overrides', [])
        datatype = infer(key,value,date_overrides)

        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts


def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if counts.get('date-time', 0) > 0:
        return 'date-time'

    if counts.get('dict', 0) > 0:
        return 'dict'

    if counts.get('list.integer', 0) > 0:
        return 'list.integer'
    elif counts.get('list.number', 0) > 0: 
        return 'list.number'
    elif counts.get('list.date-time', 0) > 0: 
        return 'list.date-time'
    elif counts.get('list.string', 0) > 0:
        return 'list.string' 
    elif counts.get('list', 0) > 0:
        return 'list' 

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return


def generate_schema(samples, table_spec):
    counts = {}
    for sample in samples:
        # {'name' : { 'string' : 45}}
        counts = count_sample(sample, counts, table_spec)
    for key, value in counts.items():
        datatype = pick_datatype(value)
        if "list." in datatype:
            child_datatype = datatype.split(".")[-1]
            counts[key] = {
                'type': "array",
                "items": datatype_schema(child_datatype)
            }
        elif datatype == "list":
            counts[key] = {
                'type': "array",
                "items": ['null','string']
            }
        else:
            counts[key] = datatype_schema(datatype)

    return counts

def datatype_schema(datatype):
    if datatype == 'date-time':
        return {
            'anyOf': [
                {'type': ['null', 'string'], 'format': 'date-time'},
                {'type': ['null', 'string']}
            ]
        }
    elif datatype == "dict":
        return {
            "type": "object",
            "properties": {}
        }
    else:
        types = ['null', datatype]
        if datatype != 'string':
            types.append('string')
        return {
            'type': types,
        }
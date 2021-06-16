import singer

LOGGER = singer.get_logger()

#pylint: disable=too-many-return-statements
def infer(key, datum, date_overrides, check_second_call=False):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        if isinstance(datum, list):
            data_type = 'string'
            if check_second_call:
                LOGGER.warning(
                    'Unsupported type for "%s", List inside list is not supported hence will be treated as a string', key)
            elif not datum:
                data_type = 'list'
            else:
                data_type = 'list.' + \
                    infer(key, datum[0], date_overrides, True)
            return data_type

        if key in date_overrides:
            return 'date-time'

        if isinstance(datum, dict):
            return 'dict'

        try:
            int(str(datum))
            return 'integer'
        except (ValueError, TypeError):
            pass
        try:
            float(str(datum))
            return 'number'
        except (ValueError, TypeError):
            pass

    except (ValueError, TypeError):
        pass

    return 'string'


def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        date_overrides = table_spec.get('date_overrides', [])
        datatype = infer(key, value, date_overrides)

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

    list_of_datatypes = ['list.date-time', 'list.dict', 'list.integer',
                         'list.number', 'list.string', 'list', 'date-time', 'dict']

    for data_types in list_of_datatypes:
        if counts.get(data_types, 0) > 0:
            return data_types

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
        if 'list.' in datatype:
            child_datatype = datatype.split('.')[-1]
            counts[key] = {
                'anyOf': [
                    {'type': 'array', 'items': datatype_schema(
                        child_datatype)},
                    {'type': ['null', 'string']}
                ]
            }
        elif datatype == 'list':
            counts[key] = {
                'anyOf': [
                    {'type': 'array', 'items': ['null', 'string']},
                    {'type': ['null', 'string']}
                ]
            }
        else:
            counts[key] = datatype_schema(datatype)

    return counts


def datatype_schema(datatype):
    if datatype == 'date-time':
        schema = {
            'anyOf': [
                {'type': ['null', 'string'], 'format': 'date-time'},
                {'type': ['null', 'string']}
            ]
        }
    elif datatype == 'dict':
        schema = {
            'anyOf': [
                {'type': 'object', 'properties': {}},
                {'type': ['null', 'string']}
            ]
        }
    else:
        types = ['null', datatype]
        if datatype != 'string':
            types.append('string')
        schema = {
            'type': types,
        }
    return schema

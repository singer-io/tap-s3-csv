import dateutil
import pytz

import singer

LOGGER = singer.get_logger()

def convert(datum, override_type=None):
    """
    Returns tuple of (converted_data_point, json_schema_type,).
    """
    if datum is None or datum == '':
        return (None, None,)

    if override_type in (None, 'integer'):
        try:
            to_return = int(datum)
            return (to_return, 'integer',)
        except (ValueError, TypeError):
            pass

    if override_type in (None, 'number'):
        try:
            #numbers are NOT floats, they are DECIMALS
            to_return = float(datum)
            return (to_return, 'number',)
        except (ValueError, TypeError):
            pass

    if override_type == 'date-time':
        return (str(datum), 'date-time',)

    return (str(datum), 'string',)


def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        # if table_spec['name'] == 'date time min and max values':
        #     import ipdb
        #     ipdb.set_trace()
        if key not in counts:
            counts[key] = {}

        override_type = table_spec.get('schema_overrides', {}).get(key, {}).get('_conversion_type')
        (_, datatype) = convert(value, override_type)

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

        if datatype == 'date-time':
            counts[key] = {
                'type': ['null', 'string'],
                'format': 'date-time',
            }
        else:
            counts[key] = {
                'type': ['null', datatype],
            }

    return counts

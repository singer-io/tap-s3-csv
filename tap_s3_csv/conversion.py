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
        try:
            to_return = dateutil.parser.parse(datum)

            if(to_return.tzinfo is None or
               to_return.tzinfo.utcoffset(to_return) is None):
                to_return = to_return.replace(tzinfo=pytz.utc)

            return (to_return.isoformat(), 'date-time',)
        except (ValueError, TypeError):
            pass

    return (str(datum), 'string',)


def count_sample(sample, counts):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        (_, datatype) = convert(value)

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


def generate_schema(samples):
    counts = {}
    for sample in samples:
        # {'name' : { 'string' : 45}}
        counts = count_sample(sample, counts)

    for key, value in counts.items():
        datatype = pick_datatype(value)

        if datatype == 'date-time':
            counts[key] = {
                'type': ['null', 'string'],
                'format': 'date-time',
                # '_conversion_type': 'date-time',
            }
        else:
            counts[key] = {
                'type': ['null', datatype],
                # '_conversion_type': datatype,
            }

    return counts

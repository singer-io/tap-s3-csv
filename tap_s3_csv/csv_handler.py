import codecs
import csv


def generator_wrapper(reader):
    for row in reader:
        to_return = {}
        for key, value in row.items():
            if key is None:
                key = '_s3_extra'

            formatted_key = key

            # remove non-word, non-whitespace characters
            #formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            to_return[formatted_key] = value

        yield to_return


def get_row_iterator(table_spec, file_handle, s3_path):
    # we use a protected member of the s3 object, _raw_stream, here to create
    # a generator for data from the s3 file.
    # pylint: disable=protected-access
    file_stream = codecs.iterdecode(
        file_handle._raw_stream, encoding='utf-8')

    field_names = None

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader((line.replace('\0', '') for line in file_stream), fieldnames=field_names)

    headers = set(reader.fieldnames)
    if table_spec['key_properties']:
        key_properties = set(table_spec['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('Found file "{}" missing key properties: {}, file only contains headers for fields: {}'
                            .format(s3_path, key_properties - headers, headers))

    if table_spec['date_overrides']:
        date_overrides = set(table_spec['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('Found file "{}" missing date_overrides fields: {}, file only contains headers for fields: {}'
                            .format(s3_path, date_overrides - headers, headers))

    return generator_wrapper(reader)

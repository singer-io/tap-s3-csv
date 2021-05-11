import codecs
import csv
import singer

LOGGER = singer.get_logger()

SDC_EXTRA_COLUMN = "_sdc_extra"

def generate_dict_reader(all_csv_headers, unique_headers, unique_headers_idxs, dup_headers_idxs, csv_reader):
    """
    Args:
        unique_headers list() : Unique Headers
        all_csv_headers list() : All CSV Headers (including duplicate headers)
        dup_headers_idxs list() : Indexes of Duplicate Headers in csv reader
        unique_headers_idxs list() : Indexes of Unique Headers in csv reader

    Return:
        yield row
    """

    for row in csv_reader:
        row_dict = {}
        row_length = len(row)
        all_csv_headers_length = len(all_csv_headers)
        dup_header_values = []

        if len(dup_headers_idxs) > 0:
            new_unique_headers_idxs = unique_headers_idxs
            new_dup_headers_idxs = dup_headers_idxs

            if row_length <= all_csv_headers_length:
                new_unique_headers_idxs = [index for index in unique_headers_idxs if index < row_length]
                new_dup_headers_idxs = [index for index in dup_headers_idxs if index < row_length]
            # If number of values provided in the row are greater than CSV headers
            else:
                dup_header_values = row[all_csv_headers_length:]

            # Fetching the values of only unique headers
            row_dict = dict(zip(unique_headers, map(row.__getitem__, new_unique_headers_idxs)))

            # Fetching the values of duplicate headers
            dup_header_values = list(map(row.__getitem__, new_dup_headers_idxs)) + dup_header_values

            if len(dup_header_values) > 0:
                row_dict.update({ SDC_EXTRA_COLUMN : dup_header_values})

        else:
            # If row contains more values than number of headers
            if row_length > all_csv_headers_length:
                row_dict = dict(zip(all_csv_headers, row[0:len(unique_headers)]))
                # Adding extra column values in _sdc_extra key
                row_dict.update({ SDC_EXTRA_COLUMN : row[len(unique_headers):] })

            else:
                row_dict = dict(zip(all_csv_headers, row))

        yield row_dict


def get_row_iterator(iterable, options=None):
    """Accepts an iterable, options and returns a csv.Reader object
    which can be used to yield CSV rows."""
    options = options or {}

    file_stream = codecs.iterdecode(iterable, encoding='utf-8')

    # Replace any NULL bytes in the line given to the Reader
    reader = csv.reader((line.replace('\0', '') for line in file_stream), delimiter=options.get('delimiter', ','))

    all_csv_headers = next(reader)

    unique_headers = []
    unique_headers_idxs = []
    dup_headers_idxs = []
    header_index = 0

    for header in all_csv_headers:
        if header in unique_headers:
            dup_headers_idxs.append(header_index)
        else:
            unique_headers.append(header)
            unique_headers_idxs.append(header_index)
        header_index += 1

    if len(dup_headers_idxs) > 0:
        duplicate_headers = set(map(all_csv_headers.__getitem__, dup_headers_idxs))
        LOGGER.warn("Duplicate Header(s) %s found in the csv and its value will be stored in the \"_sdc_extra\" field",duplicate_headers)

    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(unique_headers):
            raise Exception('CSV file missing required headers: {}'
                            .format(key_properties - set(unique_headers)))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(unique_headers):
            raise Exception('CSV file missing date_overrides headers: {}'
                            .format(date_overrides - set(unique_headers)))

    return generate_dict_reader(all_csv_headers, unique_headers, unique_headers_idxs, dup_headers_idxs, reader)

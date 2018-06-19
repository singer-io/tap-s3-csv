import boto3
import json
import re
import singer

import tap_s3_csv.conversion as conversion
import tap_s3_csv.csv_handler as csv_handler

LOGGER = singer.get_logger()

def get_bucket_config(bucket):
    s3_client = boto3.resource('s3')
    s3_object = s3_client.Object(bucket, 'config.json')

    try:
        LOGGER.info("Loading config.json from bucket %s", bucket)
        config = json.loads(s3_object.get()['Body'].read().decode('utf-8'))
    except:
        LOGGER.info("Could not find config.json in bucket %s, using provided config.", bucket)
        return None

    return config

def get_sampled_schema_for_table(config, table_spec):
    LOGGER.info('Sampling records to determine table schema.')

    s3_files = get_input_files_for_table(config, table_spec)

    samples = sample_files(config, table_spec, s3_files)

    metadata_schema = {
        '_s3_source_bucket': {'type': 'string'},
        '_s3_source_file': {'type': 'string'},
        '_s3_source_lineno': {'type': 'integer'},
        '_s3_extra': {'type': 'array', 'items': { 'type': 'string'}},
    }

    data_schema = conversion.generate_schema(samples)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }

def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return


def sample_file(config, table_spec, s3_path, sample_rate, max_records):
    LOGGER.info('Sampling %s (%s records, every %sth record).', s3_path, max_records, sample_rate)

    samples = []

    if table_spec['format'] == 'csv':
        file_handle = get_file_handle(config, s3_path)
        iterator = csv_handler.get_row_iterator(table_spec, file_handle)
    else:
        raise Exception("only supporting csv for now!")

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    LOGGER.info('Sampled %s records.', len(samples))

    return samples


# pylint: disable=too-many-arguments
def sample_files(config, table_spec, s3_files,
                 sample_rate=10, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for s3_file in s3_files:
        to_return += sample_file(config, table_spec, s3_file['key'],
                                 sample_rate, max_records)

        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return


def get_input_files_for_table(config, table_spec, modified_since=None):
    bucket = config['bucket']

    to_return = []

    pattern = table_spec['pattern']
    matcher = re.compile(pattern)

    LOGGER.info(
        'Checking bucket "%s" for keys matching "%s"', bucket, pattern)

    s3_objects = list_files_in_bucket(bucket, table_spec.get('search_prefix'))

    for s3_object in s3_objects:
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        LOGGER.info('Last modified: %s', last_modified)

        if(matcher.search(key) and
           (modified_since is None or modified_since < last_modified)):
            LOGGER.info('Will download key "%s"', key)
            to_return.append({'key': key, 'last_modified': last_modified})
        else:
            LOGGER.info('Will not download key "%s"', key)

    to_return = sorted(to_return, key=lambda item: item['last_modified'])

    return to_return


def list_files_in_bucket(bucket, search_prefix=None):
    s3_client = boto3.client('s3')

    s3_objects = []

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if search_prefix is not None:
        args['Prefix'] = search_prefix

    result = s3_client.list_objects_v2(**args)

    s3_objects += result['Contents']
    next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        LOGGER.info('Continuing pagination with token "%s".', next_continuation_token)

        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    LOGGER.info("Found %s files.", len(s3_objects))

    return s3_objects


def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']

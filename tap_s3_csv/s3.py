import itertools
import re
import json
import backoff
import boto3
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session
from tap_s3_csv import csv
from tap_s3_csv import conversion

LOGGER = singer.get_logger()

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info(
        "Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


class AssumeRoleProvider():
    METHOD = 'assume-role'

    def __init__(self, fetcher):
        self._fetcher = fetcher

    def load(self):
        return DeferredRefreshableCredentials(
            self._fetcher.fetch_credentials,
            self.METHOD
        )


@retry_pattern()
def setup_aws_client(config):
    role_arn = "arn:aws:iam::{}:role/{}".format(config['account_id'].replace('-', ''),
                                                config['role_name'])
    session = Session()
    fetcher = AssumeRoleCredentialFetcher(
        session.create_client,
        session.get_credentials(),
        role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'TapS3CSV',
            'ExternalId': config['external_id']
        },
        cache=JSONFileCache()
    )

    refreshable_session = Session()
    refreshable_session.register_component(
        'credential_provider',
        CredentialResolver([AssumeRoleProvider(fetcher)])
    )

    LOGGER.info("Attempting to assume_role on RoleArn: %s", role_arn)
    boto3.setup_default_session(botocore_session=refreshable_session)


def get_sampled_schema_for_table(config, table_spec):
    LOGGER.info('Sampling records to determine table schema.')

    s3_files_gen = get_input_files_for_table(config, table_spec)

    samples = [sample for sample in sample_files(
        config, table_spec, s3_files_gen)]

    if not samples:
        return {}

    metadata_schema = {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}}
    }

    data_schema = conversion.generate_schema(samples, table_spec)

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


def get_records_for_csv(s3_path, sample_rate, iterator):

    current_row = 0
    sampled_row_count = 0

    for row in iterator:

        if (current_row % sample_rate) == 0:
            if row.get(csv.SDC_EXTRA_COLUMN):
                row.pop(csv.SDC_EXTRA_COLUMN)
            sampled_row_count += 1
            if (sampled_row_count % 200) == 0:
                LOGGER.info("Sampled %s rows from %s",
                            sampled_row_count, s3_path)
            yield row

        current_row += 1

    LOGGER.info("Sampled %s rows from %s", sampled_row_count, s3_path)


def get_records_for_jsonl(s3_path, sample_rate, iterator):

    current_row = 0
    sampled_row_count = 0

    for row in iterator:

        if (current_row % sample_rate) == 0:
            decoded_row = row.decode('utf-8')
            if decoded_row.strip():
                row = json.loads(decoded_row)
            else:
                current_row += 1
                continue
            sampled_row_count += 1
            if (sampled_row_count % 200) == 0:
                LOGGER.info("Sampled %s rows from %s",
                            sampled_row_count, s3_path)
            yield row

        current_row += 1

    LOGGER.info("Sampled %s rows from %s", sampled_row_count, s3_path)


def check_key_properties_and_date_overrides_for_jsonl_file(table_spec, jsonl_sample_records, s3_path):

    all_keys = set()
    for record in jsonl_sample_records:
        keys = record.keys()
        all_keys.update(keys)

    if table_spec.get('key_properties'):
        key_properties = set(table_spec['key_properties'])
        if not key_properties.issubset(all_keys):
            raise Exception('JSONL file "{}" is missing required key_properties key: {}'
                            .format(s3_path, key_properties - all_keys))

    if table_spec.get('date_overrides'):
        date_overrides = set(table_spec['date_overrides'])
        if not date_overrides.issubset(all_keys):
            raise Exception('JSONL file "{}" is missing date_overrides key: {}'
                            .format(s3_path, date_overrides - all_keys))


def sample_file(config, table_spec, s3_path, sample_rate):

    file_handle = get_file_handle(config, s3_path)._raw_stream

    extension = s3_path.split(".")[-1].lower()

    records = []

    if extension in  ("csv","txt"):
        iterator = csv.get_row_iterator(
            file_handle, table_spec)  # pylint:disable=protected-access
        records = get_records_for_csv(s3_path, sample_rate, iterator)
    elif extension == "jsonl":
        iterator = file_handle
        records = get_records_for_jsonl(
            s3_path, sample_rate, iterator)
        check_jsonl_sample_records, records = itertools.tee(
            records)
        jsonl_sample_records = list(check_jsonl_sample_records)
        if len(jsonl_sample_records) == 0:
            LOGGER.exception(
                'No row sampled, Please check your JSONL file %s', s3_path)
            raise Exception(
                'No row sampled, Please check your JSONL file {}'.format(s3_path))
        check_key_properties_and_date_overrides_for_jsonl_file(
            table_spec, jsonl_sample_records, s3_path)
    else:
        LOGGER.warning(
            "'%s' having the '.%s' extension will not be sampled.", s3_path, extension)
    return records


# pylint: disable=too-many-arguments
def sample_files(config, table_spec, s3_files,
                 sample_rate=5, max_records=1000, max_files=5):
    LOGGER.info("Sampling files (max files: %s)", max_files)
    for s3_file in itertools.islice(s3_files, max_files):
        LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                    s3_file['key'],
                    max_records,
                    sample_rate)
        yield from itertools.islice(sample_file(config, table_spec, s3_file['key'], sample_rate), max_records)


def get_input_files_for_table(config, table_spec, modified_since=None):
    bucket = config['bucket']

    to_return = []

    pattern = table_spec['search_pattern']
    try:
        matcher = re.compile(pattern)
    except re.error as e:
        raise ValueError(
            ("search_pattern for table `{}` is not a valid regular "
             "expression. See "
             "https://docs.python.org/3.5/library/re.html#regular-expression-syntax").format(table_spec['table_name']),
            pattern) from e

    LOGGER.info(
        'Checking bucket "%s" for keys matching "%s"', bucket, pattern)

    matched_files_count = 0
    unmatched_files_count = 0
    max_files_before_log = 30000
    for s3_object in list_files_in_bucket(bucket, table_spec.get('search_prefix')):
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        if s3_object['Size'] == 0:
            LOGGER.info('Skipping matched file "%s" as it is empty', key)
            unmatched_files_count += 1
            continue

        if matcher.search(key):
            matched_files_count += 1
            if modified_since is None or modified_since < last_modified:
                LOGGER.info('Will download key "%s" as it was last modified %s',
                            key,
                            last_modified)
                yield {'key': key, 'last_modified': last_modified}
        else:
            unmatched_files_count += 1

        if (unmatched_files_count + matched_files_count) % max_files_before_log == 0:
            # Are we skipping greater than 50% of the files?
            if (unmatched_files_count / (matched_files_count + unmatched_files_count)) > 0.5:
                LOGGER.warn(("Found %s matching files and %s non-matching files. "
                             "You should consider adding a `search_prefix` to the config "
                             "or removing non-matching files from the bucket."),
                            matched_files_count, unmatched_files_count)
            else:
                LOGGER.info("Found %s matching files and %s non-matching files",
                            matched_files_count, unmatched_files_count)

    if matched_files_count == 0:
        raise Exception("No files found matching pattern {}".format(pattern))


@retry_pattern()
def list_files_in_bucket(bucket, search_prefix=None):
    s3_client = boto3.client('s3')

    s3_object_count = 0

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if search_prefix is not None:
        args['Prefix'] = search_prefix

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = 0
    for page in paginator.paginate(**args):
        pages += 1
        LOGGER.debug("On page %s", pages)
        s3_object_count += len(page['Contents'])
        yield from page['Contents']

    if s3_object_count > 0:
        LOGGER.info("Found %s files.", s3_object_count)
    else:
        LOGGER.warning(
            'Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)


@retry_pattern()
def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']

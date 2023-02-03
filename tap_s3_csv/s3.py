import itertools
import re
import io
import json
import gzip
import sys
import backoff
import boto3
import singer
from enum import Enum

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session
from singer_encodings import compression

from tap_s3_csv import (
    utils,
    conversion,
    csv_iterator
)

LOGGER = singer.get_logger()

skipped_files_count = 0


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

    if skipped_files_count:
        LOGGER.warning(
            "%s files got skipped during the last sampling.", skipped_files_count)

    if not samples:
        # Return empty properties for accept everything from data if no samples found
        return {
            'type': 'object',
            'properties': {}
        }

    data_schema, date_format_map = conversion.generate_schema(
        samples, table_spec, config.get('string_max_length', False))

    sampled_schema = {
        'type': 'object',
        'properties': data_schema
    }
    return sampled_schema, date_format_map


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


def maximize_csv_field_width():

    current_field_size_limit = csv_iterator.csv.field_size_limit()
    field_size_limit = sys.maxsize

    if current_field_size_limit != field_size_limit:
        csv_iterator.csv.field_size_limit(field_size_limit)
        LOGGER.info("Changed the CSV field size limit from %s to %s",
                    current_field_size_limit,
                    field_size_limit)


def get_records_for_csv(s3_path, sample_rate, iterator):

    current_row = 0
    sampled_row_count = 0

    maximize_csv_field_width()

    for row in iterator:

        # Skipping the empty line of CSV.
        if len(row) == 0:
            current_row += 1
            continue

        if (current_row % sample_rate) == 0:
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
                # Skipping the empty json.
                if len(row) == 0:
                    current_row += 1
                    continue
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

# pylint: disable=global-statement


def sampling_gz_file(table_spec, s3_path, file_handle, sample_rate):
    global skipped_files_count
    if s3_path.endswith(".tar.gz"):
        LOGGER.warning(
            'Skipping "%s" file as .tar.gz extension is not supported', s3_path)
        skipped_files_count = skipped_files_count + 1
        return []

    file_bytes = file_handle.read()
    gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(file_bytes))

    try:
        gz_file_name = utils.get_file_name_from_gzfile(
            fileobj=io.BytesIO(file_bytes))
    except AttributeError as err:
        # If a file is compressed using gzip command with --no-name attribute,
        # It will not return the file name and timestamp. Hence we will skip such files.
        # We also seen this issue occur when tar is used to compress the file
        LOGGER.warning(
            'Skipping "%s" file as we did not get the original file name', s3_path)
        skipped_files_count = skipped_files_count + 1
        return []

    if gz_file_name:
        if gz_file_name.endswith(".gz"):
            LOGGER.warning(
                'Skipping "%s" file as it contains nested compression.', s3_path)
            skipped_files_count = skipped_files_count + 1
            return []

        gz_file_extension = gz_file_name.split(".")[-1].lower()
        return sample_file(table_spec, s3_path + "/" + gz_file_name, io.BytesIO(gz_file_obj.read()), sample_rate, gz_file_extension)

    raise Exception('"{}" file has some error(s)'.format(s3_path))

# pylint: disable=global-statement


def sample_file(table_spec, s3_path, file_handle, sample_rate, extension):
    global skipped_files_count

    # Check whether file is without extension or not
    if not extension or s3_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be sampled.', s3_path)
        skipped_files_count = skipped_files_count + 1
        return []
    if extension in ["csv", "txt"]:
        # If file object read from s3 bucket file else use extracted file object from zip or gz
        iterator = csv_iterator.get_row_iterator(file_handle, table_spec)
        csv_records = []
        if iterator:
            csv_records = get_records_for_csv(s3_path, sample_rate, iterator)
        else:
            LOGGER.warning('Skipping "%s" file as it is empty', s3_path)
            skipped_files_count = skipped_files_count + 1
        return csv_records
    if extension == "gz":
        return sampling_gz_file(table_spec, s3_path, file_handle, sample_rate)
    if extension == "jsonl":
        # If file object read from s3 bucket file else use extracted file object from zip or gz
        file_handle = file_handle._raw_stream if hasattr(
            file_handle, "_raw_stream") else file_handle
        records = get_records_for_jsonl(
            s3_path, sample_rate, file_handle)
        check_jsonl_sample_records, records = itertools.tee(
            records)
        jsonl_sample_records = list(check_jsonl_sample_records)
        if len(jsonl_sample_records) == 0:
            LOGGER.warning('Skipping "%s" file as it is empty', s3_path)
            skipped_files_count = skipped_files_count + 1
        check_key_properties_and_date_overrides_for_jsonl_file(
            table_spec, jsonl_sample_records, s3_path)

        return records
    if extension == "zip":
        LOGGER.warning(
            'Skipping "%s" file as it contains nested compression.', s3_path)
        skipped_files_count = skipped_files_count + 1
        return []
    LOGGER.warning(
        '"%s" having the ".%s" extension will not be sampled.', s3_path, extension)
    skipped_files_count = skipped_files_count + 1
    return []

# pylint: disable=global-statement


def get_files_to_sample(config, s3_files, max_files):
    """
    Returns the list of files for sampling, it checks the s3_files whether any zip or gz file exists or not
    if exists then extract if and append in the list of files

    Args:
        config dict(): Configuration
        s3_files list(): List of S3 Bucket files
    Returns:
        list(dict()) : List of Files for sampling
             |_ s3_path str(): S3 Bucket File path
             |_ file_handle StreamingBody(): file object
             |_ type str(): Type of file which is used for extracted file
             |_ extension str(): extension of file (for normal files only)
    """
    global skipped_files_count
    sampled_files = []

    OTHER_FILES = ["csv", "gz", "jsonl", "txt"]

    for s3_file in s3_files:
        file_key = s3_file.get('key')

        if len(sampled_files) >= max_files:
            break

        if file_key:
            file_name = file_key.split("/").pop()
            extension = file_name.split(".").pop().lower()
            file_handle = get_file_handle(config, file_key)

            # Check whether file is without extension or not
            if not extension or file_name.lower() == extension:
                LOGGER.warning(
                    '"%s" without extension will not be sampled.', file_key)
                skipped_files_count = skipped_files_count + 1
            elif file_key.endswith(".tar.gz"):
                LOGGER.warning(
                    'Skipping "%s" file as .tar.gz extension is not supported', file_key)
                skipped_files_count = skipped_files_count + 1
            elif extension == "zip":
                files = compression.infer(
                    io.BytesIO(file_handle.read()), file_name)

                # Add only those extracted files which are supported by tap
                # Prepare dictionary contains the zip file name, type i.e. unzipped and file object of extracted file
                sampled_files.extend([{"type": "unzipped", "s3_path": file_key, "file_handle": de_file} for de_file in files if de_file.name.split(
                    ".")[-1].lower() in OTHER_FILES and not de_file.name.endswith(".tar.gz")])
            elif extension in OTHER_FILES:
                # Prepare dictionary contains the s3 file path, extension of file and file object
                sampled_files.append(
                    {"s3_path": file_key, "file_handle": file_handle, "extension": extension})
            else:
                LOGGER.warning(
                    '"%s" having the ".%s" extension will not be sampled.', file_key, extension)
                skipped_files_count = skipped_files_count + 1

    return sampled_files


# pylint: disable=too-many-arguments,global-statement
def sample_files(config, table_spec, s3_files,
                 sample_rate=1, max_records=1000, max_files=5):
    global skipped_files_count
    LOGGER.info("Sampling files (max files: %s)", max_files)

    for s3_file in itertools.islice(get_files_to_sample(config, s3_files, max_files), max_files):

        s3_path = s3_file.get("s3_path", "")
        file_handle = s3_file.get("file_handle")
        file_type = s3_file.get("type")
        extension = s3_file.get("extension")

        # Check whether the file is extracted from zip file.
        if file_type and file_type == "unzipped":
            # Append the extracted file name with zip file.
            s3_path += "/" + file_handle.name
            extension = file_handle.name.split(".")[-1].lower()

        LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                    s3_path,
                    max_records,
                    sample_rate)
        try:
            yield from itertools.islice(sample_file(table_spec, s3_path, file_handle, sample_rate, extension), max_records)
        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            # UnicodeDecodeError will be raised if non csv file parsed to csv parser
            # JSONDecodeError will be reaised if non JSONL file parsed to JSON parser
            # Handled both error and skipping file with wrong extension.
            LOGGER.warn(
                "Skipping %s file as parsing failed. Verify an extension of the file.", s3_path)
            skipped_files_count = skipped_files_count + 1

# pylint: disable=global-statement


def get_input_files_for_table(config, table_spec, modified_since=None):
    global skipped_files_count
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
    for s3_object in list_files_in_bucket(bucket, table_spec.get('search_prefix'), table_spec.get('recursive_search')):
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        if s3_object['Size'] == 0:
            LOGGER.warning('Skipping matched file "%s" as it is empty', key)
            skipped_files_count = skipped_files_count + 1
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
def list_files_in_bucket(bucket, search_prefix=None, recursive_search=True):
    s3_client = boto3.client('s3')

    s3_object_count = 0

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if not recursive_search:
        if search_prefix is not None and search_prefix != "" and not search_prefix.endswith('/'):
            search_prefix += '/'
        # This will limit results to the exact folder specified by the prefix, without going into subfolders
        args['Delimiter'] = '/'

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


class EOLType(Enum):
    LF = 1
    CR = 2
    CRLF = 3


class GetFileRangeStream:
    def __init__(self, bucket: str, key: str, start_byte: int, end_byte: int, chunk_size: int):
        if (start_byte > end_byte):
            raise ValueError(
                f'start and end byte range is invalid')

        self.bucket = bucket
        self.key = key
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.chunk_size = chunk_size

    def iter_lines(self):
        # get file size
        file_size = self.__get_content_length__()
        LOGGER.info(f'total file_size: {file_size}')

        if (self.start_byte > file_size - 1):
            raise ValueError(
                f'start byte should be smaller than file size {file_size}')

        # always return header first, after this each chunk iteration will discard first row but include partial row at the end
        headers = self.__get_headers__()
        LOGGER.info(f'headers: {headers}')
        yield headers

        eol = self.__get_eol__()

        end = min(self.end_byte, file_size - 1)

        # Assumption chunk_size can accomodate at least one row
        count_skipped_rows_at_start = 0
        pending = b''
        iter_chunks = self.__get_object_iter_chunks__(
            iter_start_byte=self.start_byte, iter_end_byte=end)
        for chunk in iter_chunks:
            lines = (pending + chunk).splitlines(True)
            for line in lines[:-1]:
                # handle partial line at chunk start
                # always assume first row is incomplete, discard first line because we do not know if its full row or not
                # this discarded first row of this range, would be included with range before this
                # for start of file, its assumed row header is discarded
                if count_skipped_rows_at_start == 0:
                    count_skipped_rows_at_start += 1
                    continue
                yield line.splitlines(False)[0]
            pending = lines[-1]

        # handle partial line at chunk end
        if pending:
            #LOGGER.info(f'pending: {pending}')
            # if its EOF, return existing pending
            if (end == file_size - 1):
                if count_skipped_rows_at_start == 0:
                    return
                yield pending.splitlines(False)[0]
                return

            start = self.end_byte + 1
            end = min(start+self.chunk_size, file_size - 1)
            iter_chunks = self.__get_object_iter_chunks__(
                iter_start_byte=start, iter_end_byte=end)

            overflow_rows_allowed = 1
            # if the end was exactly at EOL, need to return 2 end rows
            # else just need to return 1 end row
            # if EOL=\r\n, when endbyte is on \r, need to get only 1 item as next range start will skip /n instead of actual line
            if eol == EOLType.CRLF:
                if b'\r\n' in pending:
                    overflow_rows_allowed = 2
                elif b'\r' in pending:
                    overflow_row_count = 1
            elif (b'\n' in pending or b'\r' in pending):
                overflow_rows_allowed = 2

            #LOGGER.info(f'overflow_rows_allowed: {overflow_rows_allowed}')
            overflow_row_count = 0
            for chunk in iter_chunks:
                #LOGGER.info('chunk for pending', chunk)
                lines = (pending + chunk).splitlines(True)
                for line in lines[:-1]:
                    if overflow_row_count >= overflow_rows_allowed:
                        return
                    overflow_row_count += 1
                    if count_skipped_rows_at_start == 0:
                        count_skipped_rows_at_start += 1
                        continue
                    return_line = line.splitlines(False)[0]
                    if len(return_line) > 0:
                        yield return_line
                pending = lines[-1]

            if overflow_row_count >= overflow_rows_allowed or count_skipped_rows_at_start == 0:
                return
            if pending:
                #LOGGER.info(f'still pending: {pending}')
                yield pending.splitlines(False)[0]
                return

    @retry_pattern()
    def __get_object_iter_chunks__(self, iter_start_byte: int, iter_end_byte: int):
        s3 = boto3.resource('s3')
        s3_object = s3.Object(self.bucket, self.key)
        start_range = iter_start_byte
        end_range = min(iter_start_byte+self.chunk_size, iter_end_byte)

        count_s3_calls = 0
        while start_range < iter_end_byte:
            count_s3_calls += 1
            request_range = f'bytes={start_range}-{end_range}'
            #LOGGER.info(f'request range: {request_range}')
            response = s3_object.get(
                Range=request_range
            )
            for chunk in response['Body']:
                #LOGGER.info(f'chunk: {chunk}')
                yield chunk
            start_range = end_range + 1
            end_range = min(start_range+self.chunk_size, iter_end_byte)

        #LOGGER.info(f'total no of S3 calls: {count_s3_calls}')

    @retry_pattern()
    def __get_headers__(self):
        iter_chunks = self.__get_object_iter_chunks__(
            iter_start_byte=0, iter_end_byte=self.chunk_size)

        header_row = b''
        pending = b''
        for chunk in iter_chunks:
            if header_row == b'':
                lines = (pending + chunk).splitlines(True)
                for line in lines[:-1]:
                    return line.splitlines(False)[0]
                pending = lines[-1]

    @retry_pattern()
    def __get_eol__(self):
        iter_chunks = self.__get_object_iter_chunks__(
            iter_start_byte=0, iter_end_byte=self.chunk_size)

        eol_counts = {EOLType.LF: 0, EOLType.CR: 0, EOLType.CRLF: 0}
        lf = ord('\n')
        cr = ord('\r')
        prev_char = b''

        for chunk in iter_chunks:
            for char in chunk:
                if (char == cr):
                    eol_counts[EOLType.CR] += 1
                elif char == lf:
                    if prev_char == cr:
                        eol_counts[EOLType.CRLF] += 1
                        eol_counts[EOLType.CR] -= 1
                    else:
                        eol_counts[EOLType.LF] += 1
                prev_char = char

        # find maximum occuring EOL
        max_eol_type = EOLType.LF
        max_value = 0
        for key, value in eol_counts.items():
            if (value > max_value):
                max_eol_type = key
                max_value = value

        LOGGER.info(f'{max_eol_type}: {max_value}')
        return max_eol_type

    @retry_pattern()
    def __get_content_length__(self):
        s3 = boto3.resource('s3')
        s3_object = s3.Object(self.bucket, self.key)
        return s3_object.content_length


def get_csv_file(bucket: str, key: str, start: int, end: int, range_size: int):
    return GetFileRangeStream(bucket=bucket, key=key,
                              start_byte=start, end_byte=end, chunk_size=range_size)

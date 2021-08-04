import sys
import csv
import io
import json
import gzip

from singer import metadata
from singer import Transformer
from singer import utils as singer_utils

import singer
from singer_encodings import (
    compression,
    csv as csv_helper
)
from tap_s3_csv import (
    utils,
    s3
)

LOGGER = singer.get_logger()

def sync_stream(config, state, table_spec, stream):
    table_name = table_spec['table_name']
    modified_since = singer_utils.strptime_with_tz(singer.get_bookmark(state, table_name, 'modified_since') or
                                            config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3.get_input_files_for_table(
        config, table_spec, modified_since)

    records_streamed = 0

    # We sort here so that tracking the modified_since bookmark makes
    # sense. This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item['last_modified']):
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, stream)

        state = singer.write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    if s3.skipped_files_count:
        LOGGER.warn("%s files got skipped during the last sync.",s3.skipped_files_count)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed


def sync_table_file(config, s3_path, table_spec, stream):

    extension = s3_path.split(".")[-1].lower()

    # Check whether file is without extension or not
    if not extension or s3_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0
    try:
        if extension == "zip":
            return sync_compressed_file(config, s3_path, table_spec, stream)
        if extension in ["csv", "gz", "jsonl", "txt"]:
            return handle_file(config, s3_path, table_spec, stream, extension)
        LOGGER.warning('"%s" having the ".%s" extension will not be synced.',s3_path,extension)
    except (UnicodeDecodeError,json.decoder.JSONDecodeError):
        # UnicodeDecodeError will be raised if non csv file passed to csv parser
        # JSONDecodeError will be raised if non JSONL file passed to JSON parser
        # Handled both error and skipping file with wrong extension.
        LOGGER.warning("Skipping %s file as parsing failed. Verify an extension of the file.",s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
    return 0


# pylint: disable=too-many-arguments
def handle_file(config, s3_path, table_spec, stream, extension, file_handler = None):
    """
    Used to sync normal supported files
    """

    # Check whether file is without extension or not
    if not extension or s3_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0
    if extension == "gz":
        return sync_gz_file(config, s3_path, table_spec, stream, file_handler)

    if extension in ["csv", "txt"]:

        # If file is extracted from zip or gz use file object else get file object from s3 bucket
        file_handle = file_handler if file_handler else s3.get_file_handle(config, s3_path)._raw_stream #pylint:disable=protected-access
        return sync_csv_file(config, file_handle, s3_path, table_spec, stream)

    if extension == "jsonl":

        # If file is extracted from zip or gz use file object else get file object from s3 bucket
        file_handle = file_handler if file_handler else s3.get_file_handle(config, s3_path)._raw_stream
        records =  sync_jsonl_file(config, file_handle, s3_path, table_spec, stream)
        if records == 0:
            # Only space isn't the valid JSON but it is a valid CSV header hence skipping the jsonl file with only space.
            s3.skipped_files_count = s3.skipped_files_count + 1
            LOGGER.warning('Skipping "%s" file as it is empty', s3_path)
        return records

    if extension == "zip":
        LOGGER.warning('Skipping "%s" file as it contains nested compression.',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    LOGGER.warning('"%s" having the ".%s" extension will not be synced.',s3_path,extension)
    s3.skipped_files_count = s3.skipped_files_count + 1
    return 0


def sync_gz_file(config, s3_path, table_spec, stream, file_handler):
    if s3_path.endswith(".tar.gz"):
        LOGGER.warning('Skipping "%s" file as .tar.gz extension is not supported',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    # If file is extracted from zip use file object else get file object from s3 bucket
    file_object = file_handler if file_handler else s3.get_file_handle(config, s3_path)

    file_bytes = file_object.read()
    gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(file_bytes))

    # pylint: disable=duplicate-code
    try:
        gz_file_name = utils.get_file_name_from_gzfile(fileobj=io.BytesIO(file_bytes))
    except AttributeError as err:
        # If a file is compressed using gzip command with --no-name attribute,
        # It will not return the file name and timestamp. Hence we will skip such files.
        # We also seen this issue occur when tar is used to compress the file
        LOGGER.warning('Skipping "%s" file as we did not get the original file name',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    if gz_file_name:

        if gz_file_name.endswith(".gz"):
            LOGGER.warning('Skipping "%s" file as it contains nested compression.',s3_path)
            s3.skipped_files_count = s3.skipped_files_count + 1
            return 0

        gz_file_extension = gz_file_name.split(".")[-1].lower()
        return handle_file(config, s3_path + "/" + gz_file_name, table_spec, stream, gz_file_extension, io.BytesIO(gz_file_obj.read()))

    raise Exception('"{}" file has some error(s)'.format(s3_path))


def sync_compressed_file(config, s3_path, table_spec, stream):
    LOGGER.info('Syncing Compressed file "%s".', s3_path)

    records_streamed = 0
    s3_file_handle = s3.get_file_handle(config, s3_path)

    decompressed_files = compression.infer(io.BytesIO(s3_file_handle.read()), s3_path)

    for decompressed_file in decompressed_files:
        extension = decompressed_file.name.split(".")[-1].lower()

        if extension in ["csv", "jsonl", "gz", "txt"]:
            # Append the extracted file name with zip file.
            s3_file_path = s3_path + "/" + decompressed_file.name

            records_streamed += handle_file(config, s3_file_path, table_spec, stream, extension, file_handler=decompressed_file)

    return records_streamed


def sync_csv_file(config, file_handle, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)

    if "properties" in stream["schema"]:
        iterator = csv_helper.get_row_iterator(
            file_handle, table_spec, stream["schema"]["properties"].keys(), True)
    else:
        iterator = csv_helper.get_row_iterator(file_handle, table_spec, None, True)

    records_synced = 0

    if iterator:
        for row in iterator:

            #Skipping the empty line of CSV
            if len(row) == 0:
                continue

            custom_columns = {
                s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
                s3.SDC_SOURCE_FILE_COLUMN: s3_path,

                # index zero, +1 for header row
                s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
            }
            rec = {**row, **custom_columns}

            with Transformer() as transformer:
                to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(table_name, to_write)
            records_synced += 1
    else:
        LOGGER.warning('Skipping "%s" file as it is empty',s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1

    return records_synced


def sync_jsonl_file(config, iterator, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    records_synced = 0

    for row in iterator:

        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
            # Skipping the empty json row.
            if len(row) == 0:
                continue
        else:
            continue

        custom_columns = {
            s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            s3.SDC_SOURCE_FILE_COLUMN: s3_path,

            # index zero and then starting from 1
            s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 1
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))
        # collecting the value which was removed in transform to add those in _sdc_extra
        value = [ {field:rec[field]} for field in set(rec) - set(to_write) ]

        if value:
            LOGGER.warning(
                "\"%s\" is not found in catalog and its value will be stored in the \"_sdc_extra\" field.", value)
            extra_data = {
                s3.SDC_EXTRA_COLUMN: value
            }
            update_to_write = {**to_write,**extra_data}
        else:
            update_to_write = to_write

        # Transform again to validate _sdc_extra value.
        with Transformer() as transformer:
            update_to_write = transformer.transform(update_to_write, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, update_to_write)
        records_synced += 1

    return records_synced

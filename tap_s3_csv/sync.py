import sys
import csv
import io
import json
import gzip

from singer import metadata
from singer import utils as singer_utils

import singer
from singer_encodings import compression
from tap_s3_csv import (
    utils,
    s3,
    csv_iterator,
    transform,
    messages,
    preprocess
)
from tap_s3_csv.symon_exception import SymonException

LOGGER = singer.get_logger()

BUFFER_SIZE = 100


def sync_stream(config, state, table_spec, stream, start_byte, end_byte, range_size, json_lib):
    table_name = table_spec['table_name']
    bookmark = singer.get_bookmark(state, table_name, 'modified_since')
    modified_since = singer_utils.strptime_with_tz(
        bookmark or '1990-01-01T00:00:00Z')

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3.get_input_files_for_table(
        config, table_spec, modified_since)

    records_streamed = 0

    # Original implementation sorted by 'modified_since' so that the modified_since bookmark makes
    # sense. We sort by 'key' because we import multiple part files generated from Spark where the
    # names are incremental order.
    # This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item['key']):
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, stream, start_byte, end_byte, range_size, json_lib)

        state = singer.write_bookmark(
            state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    if s3.skipped_files_count:
        LOGGER.warn("%s files got skipped during the last sync.",
                    s3.skipped_files_count)

    LOGGER.info('Wrote %s records for table "%s".',
                records_streamed, table_name)

    return records_streamed


def sync_table_file(config, s3_path, table_spec, stream, byte_start, byte_end, range_size, json_lib='simple'):
    extension = s3_path.split(".")[-1].lower()

    # Check whether file is without extension or not
    if not extension or s3_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0
    try:
        if extension == "zip":
            return sync_compressed_file(config, s3_path, table_spec, stream, byte_start, byte_end, range_size)
        if extension in ["csv", "gz", "jsonl", "txt"]:
            return handle_file(config, s3_path, table_spec, stream, extension, None, byte_start, byte_end, range_size, json_lib)
        LOGGER.warning(
            '"%s" having the ".%s" extension will not be synced.', s3_path, extension)
    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
        # UnicodeDecodeError will be raised if non csv file passed to csv parser
        # JSONDecodeError will be raised if non JSONL file passed to JSON parser
        # Handled both error and skipping file with wrong extension.
        LOGGER.warning(
            "Skipping %s file as parsing failed. Verify an extension of the file.", s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
    return 0


# pylint: disable=too-many-arguments
def handle_file(config, s3_path, table_spec, stream, extension, file_handler=None, start_byte=None, end_byte=None, range_size=1024*1024, json_lib='simple'):
    """
    Used to sync normal supported files
    """

    # Check whether file is without extension or not
    if not extension or s3_path.lower() == extension:
        LOGGER.warning('"%s" without extension will not be synced.', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    if extension == "gz":
        return sync_gz_file(config, s3_path, table_spec, stream, file_handler)

    if extension in ["csv", "txt"]:
        fieldnames = None
        if file_handler:
            # If file is extracted from zip or gz use file object else get file object from s3 bucket
            file_handle = file_handler
        elif extension == 'csv' and start_byte is not None and end_byte is not None:
            file_handle = s3.get_csv_file(
                config['bucket'], s3_path, start_byte, end_byte, range_size)
            LOGGER.info('using S3 Get Range method for csv import')
            # csv.DictReader will parse the first non-empty row as header if fieldnames == None, else as the first record.
            # For parallel threads, non-first threads will not be able to grab headers from the first part of the data,
            # so we need to pass in fieldnames. First thread needs to handle first row if table_spec.has_header == True in order to avoid
            # having first row parsed as record when it's actually header. Set handle_first_row param for PreprocessStream to True
            # for this case so that the file/stream pointer is moved to skip first row.
            file_handle = preprocess.PreprocessStream(
                file_handle, table_spec, start_byte == 0 and table_spec.get('has_header', True))
            fieldnames = stream['column_order']
        else:
            file_handle = s3.get_file_handle(config, s3_path)
            if 'column_order' in stream:
                # same as above but for single thread. Set handle_first_row param to True if table_spec.has_header == True to avoid
                # having header row parsed as first record
                file_handle = preprocess.PreprocessStream(
                    file_handle, table_spec, table_spec.get('has_header', True))
                fieldnames = stream['column_order']
            else:
                # If column_order isn't present, that means we didn't do discovery with this tap - this occurs during TQP imports
                # Pass parameters to PreprocessStream to guarantee header property is set, so we can use it in place of 'column_order'
                file_handle = preprocess.PreprocessStream(
                    file_handle, table_spec, True, s3_path, config)
                fieldnames = file_handle.header

        return sync_csv_file(config, file_handle, s3_path, table_spec, stream, json_lib, fieldnames)

    if extension == "jsonl":

        # If file is extracted from zip or gz use file object else get file object from s3 bucket
        file_handle = file_handler if file_handler else s3.get_file_handle(
            config, s3_path)._raw_stream
        records = sync_jsonl_file(
            config, file_handle, s3_path, table_spec, stream)
        if records == 0:
            # Only space isn't the valid JSON but it is a valid CSV header hence skipping the jsonl file with only space.
            s3.skipped_files_count = s3.skipped_files_count + 1
            LOGGER.warning('Skipping "%s" file as it is empty', s3_path)
        return records

    if extension == "zip":
        LOGGER.warning(
            'Skipping "%s" file as it contains nested compression.', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    LOGGER.warning(
        '"%s" having the ".%s" extension will not be synced.', s3_path, extension)
    s3.skipped_files_count = s3.skipped_files_count + 1
    return 0


def sync_gz_file(config, s3_path, table_spec, stream, file_handler):
    if s3_path.endswith(".tar.gz"):
        LOGGER.warning(
            'Skipping "%s" file as .tar.gz extension is not supported', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    # If file is extracted from zip use file object else get file object from s3 bucket
    file_object = file_handler if file_handler else s3.get_file_handle(
        config, s3_path)

    file_bytes = file_object.read()
    gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(file_bytes))

    # pylint: disable=duplicate-code
    try:
        gz_file_name = utils.get_file_name_from_gzfile(
            fileobj=io.BytesIO(file_bytes))
    except AttributeError as err:
        # If a file is compressed using gzip command with --no-name attribute,
        # It will not return the file name and timestamp. Hence we will skip such files.
        # We also seen this issue occur when tar is used to compress the file
        LOGGER.warning(
            'Skipping "%s" file as we did not get the original file name', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1
        return 0

    if gz_file_name:

        if gz_file_name.endswith(".gz"):
            LOGGER.warning(
                'Skipping "%s" file as it contains nested compression.', s3_path)
            s3.skipped_files_count = s3.skipped_files_count + 1
            return 0

        gz_file_extension = gz_file_name.split(".")[-1].lower()
        return handle_file(config, s3_path + "/" + gz_file_name, table_spec, stream, gz_file_extension, io.BytesIO(gz_file_obj.read()))

    raise Exception('"{}" file has some error(s)'.format(s3_path))


def sync_compressed_file(config, s3_path, table_spec, stream):
    LOGGER.info('Syncing Compressed file "%s".', s3_path)

    records_streamed = 0
    s3_file_handle = s3.get_file_handle(config, s3_path)

    decompressed_files = compression.infer(
        io.BytesIO(s3_file_handle.read()), s3_path)

    for decompressed_file in decompressed_files:
        extension = decompressed_file.name.split(".")[-1].lower()

        if extension in ["csv", "jsonl", "gz", "txt"]:
            # Append the extracted file name with zip file.
            s3_file_path = s3_path + "/" + decompressed_file.name

            records_streamed += handle_file(config, s3_file_path, table_spec,
                                            stream, extension, decompressed_file)

    return records_streamed


def sync_csv_file(config, file_handle, s3_path, table_spec, stream, json_lib='simple', fieldnames=None):
    LOGGER.info('Syncing file "%s".', s3_path)

    table_name = table_spec['table_name']

    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)
    iterator = csv_iterator.get_row_iterator(
        file_handle, table_spec, fieldnames)

    records_synced = 0
    records_buffer = []

    if iterator:
        mdata = metadata.to_map(stream['metadata'])
        auto_fields, filter_fields, source_type_map = transform.resolve_filter_fields(
            mdata)

        tfm = transform.Transformer(source_type_map)
        # modify schema in-place to put null as the last type to check for
        # e.g. ['null', 'integer'] -> ['integer', 'null']
        tfm.transform_schema_recur(stream['schema'])

        try:
            for row in iterator:
                # Skipping the empty line of CSV
                if len(row) == 0:
                    continue
                # LOGGER.info(f'row: {row}')
                to_write = tfm.transform(
                    row, stream['schema'], auto_fields, filter_fields)
                tfm.cleanup()

                records_buffer.append(to_write)

                if len(records_buffer) >= BUFFER_SIZE:
                    messages.write_records(table_name, records_buffer, json_lib)
                    records_synced += len(records_buffer)
                    records_buffer.clear()
        except UnicodeError:
            raise SymonException("Sorry, we can't decode your file. Please try using UTF-8 or UTF-16 encoding for your file.", 'UnsupportedEncoding')
    else:
        LOGGER.warning('Skipping "%s" file as it is empty', s3_path)
        s3.skipped_files_count = s3.skipped_files_count + 1

    if len(records_buffer) > 0:
        messages.write_records(table_name, records_buffer, json_lib)
        records_synced += len(records_buffer)

    return records_synced


def sync_jsonl_file(config, iterator, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    table_name = table_spec['table_name']

    records_synced = 0
    records_buffer = []

    mdata = metadata.to_map(stream['metadata'])
    auto_fields, filter_fields, source_type_map = transform.resolve_filter_fields(
        mdata)

    for row in iterator:
        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
            # Skipping the empty json row.
            if len(row) == 0:
                continue
        else:
            continue

        with transform.Transformer(source_type_map) as transformer:
            to_write = transformer.transform(
                row, stream['schema'], auto_fields, filter_fields)

        records_buffer.append(to_write)

        if len(records_buffer) >= BUFFER_SIZE:
            messages.write_records(table_name, records_buffer)
            records_synced += len(records_buffer)
            records_buffer.clear()

    if len(records_buffer) > 0:
        messages.write_records(table_name, records_buffer)
        records_synced += len(records_buffer)

    return records_synced

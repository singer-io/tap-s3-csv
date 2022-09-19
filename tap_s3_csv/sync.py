from json import JSONDecodeError
from singer import metadata
from singer import Transformer
from singer import utils as singer_utils

import singer
from singer_encodings import csv
from tap_s3_csv import s3

LOGGER = singer.get_logger()


def sync_stream(s3_client, state, table_spec, stream):
    """Sync records by looping on the matching files"""
    table_name = table_spec['table_name']
    modified_since = singer_utils.strptime_with_tz(singer.get_bookmark(state, table_name, 'modified_since') or
                                                   s3_client.config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3_client.get_files(table_spec, modified_since)

    records_streamed = 0

    # We sort here so that tracking the modified_since bookmark makes
    # sense. This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item['last_modified']):
        try:
            records_streamed += sync_file(s3_client, s3_file, table_spec, stream)
        except (UnicodeDecodeError, JSONDecodeError):
            LOGGER.warning(
                'Skipping %s file as parsing failed. Verify an extension of the file.', s3_file['filepath'])
            records_streamed += 0
            csv.SKIP_FILES_COUNT += 1
            continue

        state = singer.write_bookmark(
            state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    if s3.skipped_files_count:
        LOGGER.warn("%s files got skipped during the last sync.",
                    s3.skipped_files_count)

    LOGGER.info('Wrote %s records for table "%s".',
                records_streamed, table_name)

    return records_streamed


def sync_file(s3_client, s3_file, table_spec, stream):
    """Sync a particular file on the S3 Path and write records"""

    LOGGER.info('Syncing file "%s".', s3_file['filepath'])

    try:
        file_handle = s3_client.get_file_handle(s3_file)
    except OSError:
        return 0

    bucket = s3_client.config.get('bucket')
    table_name = table_spec['table_name']
    schema_dict = stream["schema"]

    headers = None
    if "properties" in schema_dict:
        headers = schema_dict["properties"].keys()

    opts = {
        'key_properties': table_spec['key_properties'],
        'delimiter': table_spec.get('delimiter', ','),
        'file_name': s3_file['filepath'],
        'date_overrides': table_spec.get('date_overrides', '')
    }

    iterator = csv.get_row_iterators(
        file_handle, options=opts, infer_compression=True, with_duplicate_headers=True, headers_in_catalog=headers)

    records_synced = 0
    row_counter = 0
    tap_added_fields = ['_sdc_source_bucket', '_sdc_source_file', '_sdc_source_lineno', '_sdc_extra']

    for file_name, reader in iterator:
        row_counter = 0
        if reader:
            file_extension = file_name.split('.')[-1].lower()
            with Transformer() as transformer:
                # Row start for files as per the file type
                row_start_line = 2 if file_extension in ['csv', 'txt'] else 1
                for row in reader:
                    # Skipping the empty line
                    if len(row) == 0:
                        continue

                    custom_columns = {
                        s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
                        s3.SDC_SOURCE_FILE_COLUMN: file_name,
                        s3.SDC_SOURCE_LINENO_COLUMN: row_counter + row_start_line
                    }

                    # For CSV files, the '_sdc_extra' is handled by 'restkey' in 'csv.DictReader'
                    # If the file is JSONL then prepare '_sdc_extra' column
                    if file_extension == 'jsonl':
                        sdc_extra = []

                        # Get the extra fields ie. (JSON keys - fields from the catalog - fields added by the tap)
                        extra_fields = set()
                        # Create '_sdc_extra' fields if the schema is not empty
                        if schema_dict.get('properties'):
                            extra_fields = set(
                                row.keys()) - set(schema_dict.get('properties', {}).keys() - tap_added_fields)

                        # Prepare a list of extra fields
                        for extra_field in extra_fields:
                            sdc_extra.append(
                                {extra_field: row.get(extra_field)})
                        # If the record contains extra fields, then add the '_sdc_extra' column
                        if extra_fields:
                            LOGGER.warning(
                                '"%s" is not found in catalog and its value will be stored in the \"_sdc_extra\" field.', sdc_extra)
                            custom_columns['_sdc_extra'] = sdc_extra

                    rec = {**row, **custom_columns}

                    to_write = transformer.transform(
                        rec, stream['schema'], metadata.to_map(stream['metadata']))

                    singer.write_record(table_name, to_write)
                    records_synced += 1
                    row_counter += 1
        else:
            LOGGER.warning('Skipping "%s" file as it is empty', s3_file['filepath'])
            s3.skipped_files_count = s3.skipped_files_count + 1

    return records_synced

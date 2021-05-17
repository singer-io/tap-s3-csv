import sys
import csv
import json

from singer import metadata
from singer import Transformer
from singer import utils

import singer
from singer_encodings import csv as singer_encodings_csv
from tap_s3_csv import s3

LOGGER = singer.get_logger()


def sync_stream(config, state, table_spec, stream):
    table_name = table_spec['table_name']
    modified_since = utils.strptime_with_tz(singer.get_bookmark(state, table_name, 'modified_since') or
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

        state = singer.write_bookmark(
            state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".',
                records_streamed, table_name)

    return records_streamed


def sync_csv_file(config, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    s3_file_handle = s3.get_file_handle(config, s3_path)
    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)
    iterator = singer_encodings_csv.get_row_iterator(
        s3_file_handle._raw_stream, table_spec)  # pylint:disable=protected-access

    records_synced = 0

    for row in iterator:
        custom_columns = {
            s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            s3.SDC_SOURCE_FILE_COLUMN: s3_path,

            # index zero, +1 for header row
            s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(
                rec, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced


def sync_jsonl_file(config, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    file_handle = s3.get_file_handle(config, s3_path)._raw_stream
    iterator = file_handle

    records_synced = 0

    for row in iterator:

        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
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
            to_write = transformer.transform(
                rec, stream['schema'], metadata.to_map(stream['metadata']))

        # collecting the value which was removed in transform to add those in _sdc_extra
        value = [{field: rec[field]} for field in set(rec) - set(to_write)]

        if value:
            LOGGER.debug(
                "The schema does not have \"%s\" so its entry got removed in transformation and is stored in \"_sdc_extra\" field.", value)
            extra_data = {
                s3.SDC_EXTRA_COLUMN: value
            }
            update_to_write = {**to_write, **extra_data}
        else:
            update_to_write = to_write

        # Transform again to validate _sdc_extra value.
        with Transformer() as transformer:
            update_to_write = transformer.transform(
                update_to_write, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, update_to_write)
        records_synced += 1

    return records_synced


def sync_table_file(config, s3_path, table_spec, stream):

    extension = s3_path.split(".").pop().lower()

    records_synced = 0
    if extension in ("csv","txt"):
        records_synced = sync_csv_file(config, s3_path, table_spec, stream)
    elif extension == "jsonl":
        records_synced = sync_jsonl_file(config, s3_path, table_spec, stream)
    else:
        LOGGER.warning(
            "'%s' having the '.%s' extension will not be synced.", s3_path, extension)
    return records_synced

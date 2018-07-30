from singer import metadata
from singer import Transformer
from singer import utils

import singer
from singer_encodings import csv
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

    LOGGER.info('Found %s files to be synced.', len(s3_files))

    records_streamed = 0
    if not s3_files:
        return records_streamed

    for s3_file in s3_files:
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, stream)

        state = singer.write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

def sync_table_file(config, s3_path, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    s3_file_handle = s3.get_file_handle(config, s3_path)
    iterator = csv.get_row_iterator(s3_file_handle._raw_stream, table_spec) #pylint:disable=protected-access

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
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced

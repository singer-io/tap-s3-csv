from singer import metadata
from singer import Transformer
from singer import utils

import singer
import tap_s3_csv.s3 as s3
import tap_s3_csv.csv_handler as csv_handler

LOGGER = singer.get_logger()

def sync_stream(config, state, table_spec, stream):
    table_name = table_spec['name']
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

def sync_table_file(config, s3_file, table_spec, stream):
    LOGGER.info('Syncing file "%s".', s3_file)

    bucket = config['bucket']
    table_name = table_spec['name']

    s3_file_handle = s3.get_file_handle(config, s3_file)
    iterator = csv_handler.get_row_iterator(table_spec, s3_file_handle)

    records_synced = 0

    for row in iterator:
        custom_columns = {
            '_s3_source_bucket': bucket,
            '_s3_source_file': s3_file,

            # index zero, +1 for header row
            '_s3_source_lineno': records_synced + 2
        }
        rec = {**row, **custom_columns}

        # Converting the row? We should have already saved the override value into the schema at this point.
        # Or if the config has changed, should we apply it over any already existing schema?
        #to_write = [{**conversion.convert_row(row, schema), **metadata}]

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced

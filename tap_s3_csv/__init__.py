import json
import sys
import singer

from singer import metadata
from singer_encodings.csv import SKIP_FILES_COUNT
from tap_s3_csv.discover import discover_streams
from tap_s3_csv.s3 import S3Client
from tap_s3_csv.sync import sync_stream, skipped_files_count
from tap_s3_csv.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "account_id", "external_id", "role_name"]


def do_discover(s3_client):
    LOGGER.info("Starting discover")
    streams = discover_streams(s3_client)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(s3_client, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in s3_client.config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(s3_client, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    if SKIP_FILES_COUNT:
        LOGGER.warning("%s files got skipped during the last sampling.", SKIP_FILES_COUNT)

    LOGGER.info('Done syncing.')

def validate_table_config(config):
    # Parse the incoming tables config as JSON
    tables_config = json.loads(config['tables'])

    for table_config in tables_config:
        if ('search_prefix' in table_config) and (table_config.get('search_prefix') is None):
            table_config.pop('search_prefix')
        if table_config.get('key_properties') == "" or table_config.get('key_properties') is None:
            table_config['key_properties'] = []
        elif table_config.get('key_properties') and isinstance(table_config['key_properties'], str):
            table_config['key_properties'] = [s.strip() for s in table_config['key_properties'].split(',')]
        if table_config.get('date_overrides') == "" or table_config.get('date_overrides') is None:
            table_config['date_overrides'] = []
        elif table_config.get('date_overrides') and isinstance(table_config['date_overrides'], str):
            table_config['date_overrides'] = [s.strip() for s in table_config['date_overrides'].split(',')]

    # Reassign the config tables to the validated object
    return CONFIG_CONTRACT(tables_config)

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    config['tables'] = validate_table_config(config)

    s3_client = S3Client(config)
    try:
        for page in s3_client.list_files_in_bucket():
            break
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except:
        s3_client.setup_aws_client()

    if args.discover:
        do_discover(s3_client)
    elif args.properties:
        do_sync(s3_client, args.properties, args.state)


if __name__ == '__main__':
    main()

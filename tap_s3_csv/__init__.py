import json
import sys
import singer

from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "tables"]

def do_discover(config):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(config)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        # NOTE: Almost all of the above was stolen from tap-zendesk
        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Parse the inconing tables config as JSON
    con = json.loads(args.config['tables'])
    tables_config = CONFIG_CONTRACT(con)

    # Reassign the config tables to the validated object
    config = args.config
    config['tables'] = tables_config

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == '__main__':
    main()

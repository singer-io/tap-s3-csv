import json
import singer
import sys

from tap_s3_csv.discover import discover_streams
from tap_s3_csv.config import CONFIG_CONTRACT
LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "tables"]

def do_discover(config):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(config)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def do_sync(args):
    LOGGER.info('Starting sync.')

    config = tap_s3_csv.config.load(args.config)
    state = load_state(args.state)

    for table in config['tables']:
        state = sync_table(config, state, table)

    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    tables_config = CONFIG_CONTRACT(args.config['tables'])
    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(args)


if __name__ == '__main__':
    main()

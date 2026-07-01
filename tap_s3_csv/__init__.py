from datetime import datetime
import json
import sys
import singer

from singer import metadata
from singer import utils as singer_utils
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "account_id"]

# Config keys required only when the tap assumes a role (the secure default).
ROLE_CONFIG_KEYS = ["external_id", "role_name"]


def should_assume_role(config):
    """Whether the tap should assume a role before accessing S3.

    Secure by default: a role is always assumed unless the config explicitly
    opts out by setting ``assume_role`` to ``false``.
    """
    return config.get("assume_role", True) is not False


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state, sync_start_time):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        bookmark = singer.get_bookmark(state, stream_name, 'modified_since') or config['start_date']
        state = singer.set_bookmark(state, stream_name, 'modified_since', bookmark)
        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream, sync_start_time)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')

def validate_table_config(config):
    # Parse the incoming tables config as JSON
    tables_config = json.loads(config['tables'])

    for table_config in tables_config:
        if search_prefix := table_config.get('search_prefix'):
            # Root dir is implicit
            if search_prefix.startswith('/'):
                table_config['search_prefix'] = search_prefix[1:]
        else:
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
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    sync_start_time = singer_utils.strptime_with_tz(now_str)

    if not should_assume_role(config):
        # Opt-out path: skip role assumption and rely on the ambient AWS
        # credentials. boto3 uses its default session and s3fs lazily creates a
        # default S3FileSystem, so no client setup is required here.
        LOGGER.warning(
            "assume_role is disabled; accessing S3 with ambient AWS "
            "credentials without assuming a role."
        )
    else:
        missing_keys = [key for key in ROLE_CONFIG_KEYS if not config.get(key)]
        if missing_keys:
            raise Exception(
                "Assuming a role requires the following config keys: {}. "
                "Set 'assume_role' to false to access S3 with ambient AWS "
                "credentials instead.".format(", ".join(missing_keys))
            )

        if 'proxy_account_id' in config and 'proxy_role_name' in config:
            s3.setup_aws_client_with_proxy(config)
            s3.setup_s3fs_client_with_proxy(config)
        else:
            # Assume the configured role before making any AWS calls. Probing
            # for direct bucket access first would issue requests with the
            # originating account's credentials, so the client is set up upfront.
            s3.setup_aws_client(config)
            s3.setup_s3fs_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.catalog:
        do_sync(config, args.catalog.to_dict(), args.state, sync_start_time)
    elif args.properties:
        do_sync(config, args.properties, args.state, sync_start_time)


if __name__ == '__main__':
    main()

import json
import sys
import singer
import time
import traceback

from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT
from tap_s3_csv import dialect
from tap_s3_csv.symon_exception import SymonException

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["bucket"]
REQUIRED_CONFIG_KEYS_EXTERNAL_SOURCE = [
    "bucket", "account_id", "external_id", "role_name"]

IMPORT_PERF_METRICS_LOG_PREFIX = "IMPORT_PERF_METRICS:"

# for symon error logging
ERROR_START_MARKER = '[tap_error_start]'
ERROR_END_MARKER = '[tap_error_end]'


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


def do_sync(config, catalog, state):
    start_byte = config.get('start_byte')
    end_byte = config.get('end_byte')
    range_size = config.get('range_size', 1024*1024*5)
    json_lib = config.get('json_lib', 'orjson')

    LOGGER.info(f'Starting sync ({start_byte}-{end_byte}).')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(
            s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)

        key_properties = mdata.get((), {}).get('table-key-properties', [])
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(
            config, state, table_spec, stream, start_byte, end_byte, range_size, json_lib)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    # import performance logging - left here for convenience
    # timers_str = ', '.join(f'"{k}": {v:.0f}' for k, v in timers.items())
    # logMsg = f"{IMPORT_PERF_METRICS_LOG_PREFIX} {{{timers_str}}}"
    # LOGGER.info(logMsg)

    LOGGER.info('Done syncing.')


def validate_table_config(config):
    # Parse the incoming tables config as JSON
    tables_config = config['tables']

    for table_config in tables_config:
        if ('search_prefix' in table_config) and (table_config.get('search_prefix') is None):
            table_config.pop('search_prefix')
        if table_config.get('key_properties') == "" or table_config.get('key_properties') is None:
            table_config['key_properties'] = []
        elif table_config.get('key_properties') and isinstance(table_config['key_properties'], str):
            table_config['key_properties'] = [s.strip()
                                              for s in table_config['key_properties'].split(',')]
        if table_config.get('date_overrides') == "" or table_config.get('date_overrides') is None:
            table_config['date_overrides'] = []
        elif table_config.get('date_overrides') and isinstance(table_config['date_overrides'], str):
            table_config['date_overrides'] = [s.strip()
                                              for s in table_config['date_overrides'].split(',')]

    # Reassign the config tables to the validated object
    return CONFIG_CONTRACT(tables_config)


@singer.utils.handle_top_exception(LOGGER)
def main():
    try:
        # used for storing error info to write if error occurs
        error_info = None
        args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
        config = args.config

        external_source = False

        if 'external_id' in config:
            args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS_EXTERNAL_SOURCE)
            config = args.config
            external_source = True

        config['tables'] = validate_table_config(config)

        # If external_id is provided, we are trying to access files in another AWS account, and need to assume the role
        if external_source:
            s3.setup_aws_client(config)
        # Otherwise, confirm that we can access the bucket in our own AWS account
        else:
            try:
                for page in s3.list_files_in_bucket(config['bucket']):
                    break
            except BaseException as err:
                LOGGER.error(err)

            # If not external source, it is from importing csv (replacement for tap-csv)
            dialect.detect_tables_dialect(config)
        if args.discover:
            do_discover(args.config)
        elif args.properties:
            do_sync(config, args.properties, args.state)
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == '__main__':
    main()

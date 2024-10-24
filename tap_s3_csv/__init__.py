import json
import sys
import singer
import boto3

from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "proxy_external_id", "cust_external_id", "proxy_account_id", "cust_account_id", "proxy_role_name", "cust_role_name"]

def assume_role(role_arn, session_name):
    """Assume an IAM role and return temporary credentials."""
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )
    return response['Credentials']


def setup_aws_client(config):
    """Setup S3 client using assumed role credentials."""
    # Assume role in the proxy account
    proxy_role_arn = f"arn:aws:iam::{config['proxy_account_id']}:role/{config['proxy_role_name']}"
    proxy_credentials = assume_role(proxy_role_arn, 'ProxySession')

    # Use proxy credentials to assume role in the cust account
    cust_role_arn = f"arn:aws:iam::{config['cust_account_id']}:role/{config['cust_role_name']}"
    cust_credentials = assume_role(cust_role_arn, 'CustSession')

    # Create an S3 client using the cust role credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=cust_credentials['AccessKeyId'],
        aws_secret_access_key=cust_credentials['SecretAccessKey'],
        aws_session_token=cust_credentials['SessionToken']
    )
    LOGGER.info("S3 client created using assumed role credentials")
    return s3_client

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
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

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

    try:
        # s3_client = setup_aws_client(config)
        for page in s3.list_files_in_bucket(config):
            break
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except:
        s3.setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == '__main__':
    main()

def do_discover(config, state, streams):

def do_sync(args):
    LOGGER.info('Starting sync.')

    config = tap_s3_csv.config.load(args.config)
    state = load_state(args.state)

    for table in config['tables']:
        state = sync_table(config, state, table)

    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        do_discovery(config, state, streams)
    elif args.properties.get('streams', []):
        do_sync(args)


if __name__ == '__main__':
    main()

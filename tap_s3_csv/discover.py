import singer
from singer import metadata

from tap_s3_csv import s3
from tap_s3_csv.exceptions import S3CsvForbiddenError

LOGGER = singer.get_logger()


class TableStream:
    parent = None

    def __init__(self, table_spec, client=None):
        self.table_spec = table_spec
        self.client = client

    def get_url_endpoint(self):
        prefix = self.table_spec.get('search_prefix')
        if prefix:
            return "s3://{}/{}".format(self.client.config['bucket'], prefix)
        return "s3://{}".format(self.client.config['bucket'])

    def update_params(self):
        return {'search_prefix': self.table_spec.get('search_prefix')}

    def check_access(self):
        if self.parent:
            return True

        if self.client is None:
            return True

        try:
            self.client.make_request("LIST", self.update_params())
            return True
        except S3CsvForbiddenError as exc:
            LOGGER.warning(
                "Permission Error: Stream '%s' - %s",
                self.table_spec.get('table_name'),
                exc,
            )
            return False


def _prune_inaccessible_children(table_specs, schemas: dict, field_metadata: dict) -> None:
    table_map = {table_spec['table_name']: table_spec for table_spec in table_specs}
    for name, table_spec in list(table_map.items()):
        parent_name = table_spec.get('parent')
        if name in schemas and parent_name and parent_name not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name,
                parent_name,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def _apply_access_checks(client, table_specs, schemas: dict, field_metadata: dict) -> None:
    inaccessible_streams = [
        table_spec['table_name']
        for table_spec in table_specs
        if table_spec['table_name'] in schemas
        and not TableStream(table_spec=table_spec, client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(table_specs, schemas, field_metadata)

    if not schemas:
        raise S3CsvForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_streams),
        )


def discover_streams(config, client=None):
    streams = []
    schemas = {}
    field_metadata = {}

    for table_spec in config['tables']:
        stream_name = table_spec['table_name']
        schema = discover_schema(config, table_spec)
        schemas[stream_name] = schema
        field_metadata[stream_name] = load_metadata(table_spec, schema)

    if client is not None:
        _apply_access_checks(client, config['tables'], schemas, field_metadata)

    for table_spec in config['tables']:
        stream_name = table_spec['table_name']
        if stream_name not in schemas:
            continue
        streams.append({'stream': stream_name, 'tap_stream_id': stream_name,
                       'schema': schemas[stream_name], 'metadata': field_metadata[stream_name]})
    return streams


def discover_schema(config, table_spec):
    sampled_schema = s3.get_sampled_schema_for_table(config, table_spec)
    return sampled_schema


def load_metadata(table_spec, schema):
    mdata = metadata.new()

    mdata = metadata.write(
        mdata, (), 'table-key-properties', table_spec['key_properties'])

    for field_name in schema.get('properties', {}).keys():
        if table_spec.get('key_properties', []) and field_name in table_spec.get('key_properties', []):
            mdata = metadata.write(
                mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(
                mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)

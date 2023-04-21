from singer import metadata
from tap_s3_csv import s3


def discover_streams(config):
    streams = []

    for table_spec in config['tables']:
        schema, date_format_map = discover_schema(config, table_spec)
        streams.append({'stream': table_spec['table_name'], 'tap_stream_id': table_spec['table_name'], 'schema': schema, 'metadata': load_metadata(
            table_spec, schema), 'column_date_format': date_format_map, 'column_order': [str(column) for column in schema['properties']]})
    return streams


def discover_schema(config, table_spec):
    return s3.get_sampled_schema_for_table(config, table_spec)


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
        mdata = metadata.write(
            mdata, ('properties', field_name), 'source_type', 'string')             # For csv we always write this for import confirmation screen

    return metadata.to_list(mdata)

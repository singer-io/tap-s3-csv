from singer import metadata

import tap_s3_csv.s3 as s3

def discover_streams(config):
    streams = []

    for table_spec in config['tables']:
        schema = discover_schema(config, table_spec)
        streams.append({'stream': table_spec['name'], 'tap_stream_id': table_spec['name'], 'schema': schema, 'metadata': load_metadata(table_spec, schema)})
    return streams

def discover_schema(config, table_spec):
    sampled_schema = s3.get_sampled_schema_for_table(config, table_spec)
    return sampled_schema

def load_metadata(table_spec, schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', table_spec['key_properties'])
    #mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')

    #if self.replication_key:
    #    mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

    for field_name in schema['properties'].keys():
        if field_name in table_spec['key_properties']:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


# {'key_properties': ['name'], 'format': 'csv', 'name': 'chickens', 'pattern': 'csv-exports-chickens/(.*)\\.csv$'}

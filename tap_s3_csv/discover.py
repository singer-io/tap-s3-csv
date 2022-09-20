from singer import metadata
from singer_encodings import json_schema
from tap_s3_csv.s3 import SDC_EXTRA_COLUMN, SDC_SOURCE_BUCKET_COLUMN, SDC_SOURCE_FILE_COLUMN, SDC_SOURCE_LINENO_COLUMN

def get_s3_sdc_columns():
    """Override 'get_sdc_columns' from 'json_schema' as per the S3 code"""
    return {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {'type': 'array', 'items': {
            'anyOf': [{'type': 'object', 'properties': {}}, {'type': 'string'}]}}
    }

json_schema.get_sdc_columns = get_s3_sdc_columns

def discover_streams(s3_client):
    streams = []

    for table_spec in s3_client.config['tables']:
        schema = json_schema.get_schema_for_table(s3_client, table_spec, sample_rate=5)
        streams.append({'stream': table_spec['table_name'], 'tap_stream_id': table_spec['table_name'],
                       'schema': schema, 'metadata': load_metadata(table_spec, schema)})
    return streams

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

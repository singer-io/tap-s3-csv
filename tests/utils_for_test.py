import boto3
import os
import json
from tap_tester import menagerie, connections

def get_resources_path(file_path, folder_path = None):
    if folder_path:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', folder_path, file_path)
    else:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', file_path)


def delete_and_push_file(properties, resource_names, folder_path=None, search_prefix_index = 0):
    """
    Delete the file from S3 Bucket first and then upload it again

    Args:
        properties (dict) : config.json
        resource_name (list) : List of file name only (available in resources directory)
    """
    s3_client = boto3.resource('s3')

    # Parsing the properties tables is a hack for now.
    tables = json.loads(properties['tables'])

    s3_bucket = s3_client.Bucket(properties['bucket'])

    for resource_name in resource_names:

        s3_path = tables[search_prefix_index]['search_prefix'] + '/' + resource_name
        s3_object = s3_bucket.Object(s3_path)

        # Attempt to delete the file before we start
        print("Attempting to delete S3 file before test.")
        try:
            s3_object.delete()
        except:
            print("S3 File does not exist, moving on.")

        # Put S3 File to AWS S3 Bucket
        s3_object.upload_file(get_resources_path(resource_name, folder_path))


def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']

def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
    """Select all streams and all fields within streams"""
    for catalog in catalogs:
        schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

        non_selected_properties = []
        if not select_all_fields:
            # get a list of all properties so that none are selected
            non_selected_properties = schema.get('annotated-schema', {}).get(
                'properties', {}).keys()

        connections.select_catalog_and_fields_via_metadata(
            conn_id, catalog, schema, [], non_selected_properties)
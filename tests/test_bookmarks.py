import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from functools import reduce
from singer import metadata
import unittest
import boto3
import json
import os

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)

def delete_and_push_csv(properties, resource_name):
    s3_client = boto3.resource('s3')

    # Parsing the properties tables is a hack for now.
    tables = json.loads(properties['tables'])

    s3_bucket = s3_client.Bucket(properties['bucket'])
    s3_path = tables[0]['search_prefix'] + '/' + resource_name
    s3_object = s3_bucket.Object(s3_path)

    # Attempt to delete the file before we start
    print("Attempting to delete S3 file before test.")
    try:
        s3_object.delete()
    except:
        print("S3 File does not exist, moving on.")

    # Put S3 File to com-stitchdata-prod-circleci-assets/s3_discovery_test
    s3_object.upload_file(get_resources_path(resource_name))


class S3Bookmarks(unittest.TestCase):

    def setUp(self):
        delete_and_push_csv(self.get_properties(), self.resource_name())
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return "bookmarks.csv"

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return "tap_tester_s3_csv_bookmarks"

    def expected_check_streams(self):
        return {
            'chickens'
        }

    def expected_sync_streams(self):
        return {
            'chickens'
        }

    def expected_pks(self):
        return {
            'chickens': {"name"}
        }

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01 00:00:00',
            'bucket': 'com-stitchdata-prod-circleci-assets',
            'account_id': '218546966473',
            'external_id': 'not-used',
            'role_name': 'dev_tap_s3',
            'tables': "[{\"table_name\": \"chickens\",\"search_prefix\": \"tap_tester\",\"search_pattern\": \"tap_tester/bookmarks.*\",\"key_properties\": \"name\"}]"
        }

    def test_run(self):
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, self.conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Put a new file to S3
        delete_and_push_csv(self.get_properties(), "bookmarks2.csv")

        # Run another Sync
        sync_job_name = runner.run_sync_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('chickens').get('messages')
        self.assertEqual(len(messages), 2, msg="Sync'd incorrect count of messages: {}".format(len(messages)))

        # Run a final sync
        sync_job_name = runner.run_sync_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('chickens', {}).get('messages', [])
        self.assertEqual(len(messages), 0, msg="Sync'd incorrect count of messages: {}".format(len(messages)))

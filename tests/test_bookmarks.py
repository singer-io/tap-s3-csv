from tap_tester import connections, menagerie, runner
from functools import reduce
from singer import metadata
import boto3
import json
import os

from base import S3CSVBaseTest

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


class S3Bookmarks(S3CSVBaseTest):

    table_entry = [{'table_name': 'chickens', 'search_prefix': 'tap_tester', 'search_pattern': 'tap_tester/bookmarks.*', 'key_properties': 'name'}]

    def setUp(self):
        delete_and_push_csv(self.get_properties(), self.resource_name())
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return "bookmarks.csv"

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

    def test_run(self):
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

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

import json
from tap_tester import connections, menagerie, runner
import unittest
from utils_for_test import delete_and_push_file, select_all_streams_and_fields

class S3CSVAllFieldsTest(unittest.TestCase):

    file_name = None
    headers = None

    def upload_file(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), 'PrimaryKey-CSV')
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return [self.file_name]

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return "test_automatic_fields"

    def expected_sync_streams(self):
        return {"csv_with_pk"}

    def expected_pks(self):
        return {{"csv_with_pk": set(self.headers.split(", "))}}

    def expected_automatic_fields(self):
        return{"csv_with_pk": set(self.headers.split(", "))}

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            "start_date" : "2021-11-02T00:00:00Z",
            "bucket": "com-stitchdata-prod-circleci-assets",
            "account_id": "218546966473",
            "tables": json.dumps([{"table_name": "csv_with_pk", "search_prefix": "tap_tester", "search_pattern": "{}".format(self.file_name), "key_properties": "{}".format(self.headers)}])
        }

    def run_test(self):
        self.upload_file()

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_sync_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        select_all_streams_and_fields(self.conn_id, our_catalogs, False)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        synced_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

    def test_CSV_with_1_PK(self):
        self.file_name = "CSV_with_one_primary_key.csv"
        self.headers = "id"
        self.run_test()

    def test_CSV_with_2_PK(self):
        self.file_name = "CSV_with_two_primary_keys.csv"
        self.headers = "id, name"
        self.run_test()

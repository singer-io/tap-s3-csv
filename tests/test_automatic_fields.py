import json
import logging
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest
from utils_for_test import delete_and_push_file, select_all_streams_and_fields

class S3CSVAllFieldsTest(S3CSVBaseTest):

    file_name = None
    headers = None

    def upload_file(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), 'PrimaryKey-CSV')
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return [self.file_name]

    def name(self):
        return "test_automatic_fields"

    def expected_sync_streams(self):
        return {"csv_with_pk"}

    def expected_check_streams(self):
        return {"csv_with_pk"}

    def expected_pks(self):
        return {"csv_with_pk": set(self.headers.split(", "))}

    def expected_automatic_fields(self):
        return{"csv_with_pk": set(self.headers.split(", "))}

    def run_test(self):
        self.upload_file()

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        select_all_streams_and_fields(self.conn_id, our_catalogs, False)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

    def test_CSV_with_1_PK(self):
        """
            Test CSV file having one Primary Key
        """
        self.file_name = "CSV_with_one_primary_key.csv"
        self.headers = "id"
        self.table_entry = [{"table_name": "csv_with_pk", "search_prefix": "tap_tester", "search_pattern": "{}".format(self.file_name), "key_properties": "{}".format(self.headers)}]
        self.run_test()

    def test_CSV_with_2_PK(self):
        """
            Test CSV file having two Primary Keys
        """
        self.file_name = "CSV_with_two_primary_keys.csv"
        self.headers = "id, name"
        self.table_entry = [{"table_name": "csv_with_pk", "search_prefix": "tap_tester", "search_pattern": "{}".format(self.file_name), "key_properties": "{}".format(self.headers)}]
        self.run_test()

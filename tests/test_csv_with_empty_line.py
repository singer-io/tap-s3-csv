import json
import unittest
from base import S3CSVBaseTest
import utils_for_test as utils
import os

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

TAP_S3_CSV_PATH = "tap-s3-csv"

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)


class S3EmptyLineInCSVTest(S3CSVBaseTest):

    table_entry = [{'table_name': 'csv_with_empty_lines', 'search_prefix': 'tap_tester', 'search_pattern': 'test_csv_with_empty_lines.csv'}]

    def resource_names(self):
        return ["test_csv_with_empty_lines.csv"]

    def name(self):
        return "tap_tester_s3_csv_with_empty_lines"

    def expected_sync_streams(self):
        return {
            'csv_with_empty_lines'
        }

    def expected_check_streams(self):
        return {
            'csv_with_empty_lines'
        }

    def expected_pks(self):
        return {
            'csv_with_empty_lines': {}
        }

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def setUpTestEnvironment(self):
        for resource in self.resource_names():
            utils.delete_and_push_file(self.get_properties(), [
                                       resource], TAP_S3_CSV_PATH)

    def test_catalog_without_properties(self):

        self.setUpCompressedEnv(TAP_S3_CSV_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        our_catalogs = [c for c in found_catalogs if c.get(
            'tap_stream_id') in self.expected_sync_streams()]

        # Select our catalogs
        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        synced_records = runner.get_records_from_target_output()
        upsert_messages = [m for m in synced_records.get(
            'csv_with_empty_lines').get('messages') if m['action'] == 'upsert']

        records = [message.get('data') for message in upsert_messages]

        #Empty line should be ignored in emitted records.

        expected_records = [
            {'id': 1, 'name': 'John', '_sdc_extra': [{'name': 'carl'}], '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_csv_with_empty_lines.csv', '_sdc_source_lineno': 2},
            {'id': 2, 'name': 'Bob', '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_csv_with_empty_lines.csv', '_sdc_source_lineno': 3},
            {'id': 3, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_csv_with_empty_lines.csv', '_sdc_source_lineno': 4},
            {'id': 4, 'name': 'Alice', '_sdc_extra': [{'no_headers': ['Ben', '5']}, {
                'name': 'Barak'}], '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/test_csv_with_empty_lines.csv', '_sdc_source_lineno': 5}
        ]

        self.assertListEqual(expected_records, records)

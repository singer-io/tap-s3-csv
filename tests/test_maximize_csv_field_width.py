import unittest
from tap_tester import menagerie, runner, connections
from base import S3CSVBaseTest
from utils_for_test import delete_and_push_file

class S3MaxFieldWidthCSV(S3CSVBaseTest):

    table_entry = [{'table_name': 'csv_with_max_field_width', 'search_prefix': 'tap_tester', 'search_pattern': 'max_size.csv'}]

    def resource_name(self):
        return ["max_size.csv"]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name())
        self.conn_id = connections.ensure_connection(self)

    def name(self):
        return "test_maximize_csv_field_width"

    def expected_check_streams(self):
        return {'csv_with_max_field_width'}

    def expected_sync_streams(self):
        return {'csv_with_max_field_width'}

    def expected_pks(self):
        return {'csv_with_max_field_width': {}}

    def test_run(self):

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        self.assertEqual(self.expected_check_streams(), found_catalog_names, msg="Expected check streams are not subset of discovered catalog")

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]
        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        expected_records = 1
        record_count = runner.get_upserts_from_target_output()
        # Verify record counts
        self.assertEqual(expected_records, len(record_count))

        records = runner.get_records_from_target_output()
        actual_records = [record.get('data') for record in records.get('csv_with_max_field_width').get('messages')]
        # Verify the record we created of length greater than 'csv.field_size_limit' of '131072' is replicated
        self.assertEqual(actual_records, [{'id': 1, 'name': '{}'.format('a'*131074), '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/max_size.csv', '_sdc_source_lineno': 2}])

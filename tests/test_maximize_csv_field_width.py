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

    def get_credentials(self):
        return {}

    def test_run(self):

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        self.assertEqual(self.expected_check_streams(), found_catalog_names, msg="Expected check streams are not subset of discovered catalog")

import unittest
from tap_tester import menagerie, runner, connections
from utils_for_test import delete_and_push_file

class S3MaxFieldWidthCSV(unittest.TestCase):

    def resource_name(self):
        return ["max_size.csv"]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name())
        self.conn_id = connections.ensure_connection(self)

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return "test_maximize_csv_field_width"

    def expected_check_streams(self):
        return {'csv_with_max_field_width'}

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'bucket': 'com-stitchdata-prod-circleci-assets',
            'account_id': '218546966473',
            'tables': "[{\"table_name\": \"csv_with_max_field_width\",\"search_prefix\": \"tap_tester\",\"search_pattern\": \"max_size.csv\"}]"
        }

    def test_run(self):

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

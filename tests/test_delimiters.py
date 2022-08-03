from decimal import Decimal
import json
import unittest
from tap_tester import menagerie, runner, connections
from base import S3CSVBaseTest
from utils_for_test import delete_and_push_file

FOLDER_PATH = "Delimiters"

class S3DelimetersBase(S3CSVBaseTest):

    file_name = None

    def name(self):
        return "test_csv_file_with_delimeter"

    def resource_name(self):
        return [self.file_name]

    def upload_file(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), FOLDER_PATH)
        self.conn_id = connections.ensure_connection(self)

    def expected_check_streams(self):
        return {"delimiters_table"}

    def expected_pks(self):
        return {"delimiters_table": {}}

    def expected_sync_streams(self):
        return {"delimiters_table"}

    def get_properties(self):
        table_entry = {"table_name": "delimiters_table", "search_prefix": "tap_tester/Delimiters", "search_pattern": "{}".format(self.file_name)}
        if self.delimeter:
            table_entry["delimiter"] = self.delimeter

        return {
            "start_date" : "2017-01-01T00:00:00Z",
            "bucket": "com-stitchdata-prod-circleci-assets",
            "account_id": "218546966473",
            "tables": json.dumps([table_entry])
        }

    def run_test(self):

        # Upload file on s3 bucket
        self.upload_file()

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]
        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        records = runner.get_records_from_target_output()
        actual_records = [record.get("data") for record in records.get("delimiters_table").get("messages")]

        expected_records = [
            {'name': 'Nicolea', 'birthday': '1923-12-15T16:57:08Z', 'gender': 'Female', 'age': 93, 'bank_balance': Decimal('855.24'), 'mixed_nums': 277, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/Delimiters/{}'.format(self.file_name), '_sdc_source_lineno': 2},
            {'name': 'Querida', 'birthday': '1958-02-03T19:50:37Z', 'gender': 'Female', 'age': 70, 'bank_balance': Decimal('745.84'), 'mixed_nums': 270, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/Delimiters/{}'.format(self.file_name), '_sdc_source_lineno': 3},
            {'name': 'Worthington', 'birthday': '1938-04-15T11:18:18Z', 'gender': 'Male', 'age': 42, 'bank_balance': Decimal('837.62'), 'mixed_nums': 206, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/Delimiters/{}'.format(self.file_name), '_sdc_source_lineno': 4},
            {'name': 'Lorne', 'birthday': '1987-09-22T17:58:39Z', 'gender': 'Male', 'age': 10, 'bank_balance': Decimal('533.49'), 'mixed_nums': 137, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/Delimiters/{}'.format(self.file_name), '_sdc_source_lineno': 5},
            {'name': 'Saundra', 'birthday': '2005-07-18T20:34:15Z', 'gender': 'Male', 'age': 76, 'bank_balance': Decimal('524.76'), 'mixed_nums': 252, '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/Delimiters/{}'.format(self.file_name), '_sdc_source_lineno': 6}
        ]

        self.assertEqual(actual_records, expected_records)

    def test_CSV_file_with_comma_delimeter(self):
        """Test csv file having comma as a delimeter"""

        self.file_name = "comma.csv"
        self.delimeter = None
        self.run_test()

    def test_CSV_file_with_pipe_delimeter(self):
        """Test csv file having pipe as a delimeter"""

        self.file_name = "pipe.csv"
        self.delimeter = "|"
        self.run_test()

    def test_CSV_file_with_semi_colon_delimeter(self):
        """Test csv file having semicolon as a delimeter"""

        self.file_name = "semi_colon.csv"
        self.delimeter = ";"
        self.run_test()

    def test_CSV_file_with_tab_delimeter(self):
        """Test csv file having tab as a delimeter"""

        self.file_name = "tab.csv"
        self.delimeter = "\t"
        self.run_test()

    def test_TXT_file_with_comma_delimeter(self):
        """Test txt file having comma as a delimeter"""

        self.file_name = "comma.txt"
        self.delimeter = None
        self.run_test()

    def test_TXT_file_with_pipe_delimeter(self):
        """Test txt file having pipe as a delimeter"""

        self.file_name = "pipe.txt"
        self.delimeter = "|"
        self.run_test()

    def test_TXT_file_with_semi_colon_delimeter(self):
        """Test txt file having semicolon as a delimeter"""

        self.file_name = "semi_colon.txt"
        self.delimeter = ";"
        self.run_test()

    def test_TXT_file_with_tab_delimeter(self):
        """Test txt file having tab as a delimeter"""

        self.file_name = "tab.txt"
        self.delimeter = "\t"
        self.run_test()

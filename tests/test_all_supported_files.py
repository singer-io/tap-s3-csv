import simplejson
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

import utils_for_test as utils
import unittest

ALL_SUPPORTED_FOLDER_PATH = "All-Supported-Files"

class S3AllFilesSupport(S3CSVBaseTest):

    table_entry = [
        {'table_name': 'all_support_csv', 'search_prefix': 'int_test_all_support_csv', 'search_pattern': 'int_test_all_support_csv\\/.*\\.csv'},
        {'table_name': 'all_support_jsonl', 'search_prefix': 'int_test_all_support_jsonl', 'search_pattern': 'int_test_all_support_jsonl\\/.*\\.jsonl'},
        {'table_name': 'all_support_gz_has_csv', 'search_prefix': 'int_test_all_support_gz_has_csv', 'search_pattern': 'int_test_all_support_gz_has_csv\\/.*\\.gz'},
        {'table_name': 'all_support_gz_has_jsonl', 'search_prefix': 'int_test_all_support_gz_has_jsonl', 'search_pattern': 'int_test_all_support_gz_has_jsonl\\/.*\\.gz'},
        {'table_name': 'all_support_zip', 'search_prefix': 'int_test_all_support_zip', 'search_pattern': 'int_test_all_support_zip\\/.*\\.zip'}
    ]

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return ["sample_csv_file.csv","sample_jsonl_file.jsonl","sample_gz_file_has_csv.gz","sample_gz_file_has_jsonl.gz","sample_zip_file.zip"]

    def name(self):
        return "test_all_supported_files"

    def expected_check_streams(self):
        return {
            "all_support_csv",
            "all_support_jsonl",
            "all_support_gz_has_csv",
            "all_support_gz_has_jsonl",
            "all_support_zip"
        }

    def expected_sync_streams(self):
        return {
            "all_support_csv",
            "all_support_jsonl",
            "all_support_gz_has_csv",
            "all_support_gz_has_jsonl",
            "all_support_zip"
        }

    def expected_pks(self):
        return {
            "all_support_csv": {},
            "all_support_jsonl": {},
            "all_support_gz_has_csv": {},
            "all_support_gz_has_jsonl": {},
            "all_support_zip": {}
        }

    def setUpTestEnvironment(self):
        index = 0
        for resource in self.resource_names():
            utils.delete_and_push_file(self.get_properties(), [resource], ALL_SUPPORTED_FOLDER_PATH, index)
            index += 1

    def test_run(self):

        self.setUpCompressedEnv(ALL_SUPPORTED_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        self.run_and_verify_sync(self.conn_id)

        no_csv_records = 998
        no_jsonl_records = 10
        no_gz_has_csv_records = 998
        no_gz_has_jsonl_records = 2
        no_zip_records = 40

        expected_records = no_csv_records + no_jsonl_records + no_gz_has_csv_records + no_gz_has_jsonl_records + no_zip_records

        with open(utils.get_resources_path("output_csv_records.json", ALL_SUPPORTED_FOLDER_PATH)) as json_file:
            expected_csv_records = simplejson.load(json_file, use_decimal = True).get("records", [])
        with open(utils.get_resources_path("output_jsonl_records.json", ALL_SUPPORTED_FOLDER_PATH)) as json_file:
            expected_jsonl_records = simplejson.load(json_file, use_decimal = True).get("records", [])
        with open(utils.get_resources_path("output_gz_csv_records.json", ALL_SUPPORTED_FOLDER_PATH)) as json_file:
            expected_gz_has_csv_records = simplejson.load(json_file, use_decimal = True).get("records", [])
        with open(utils.get_resources_path("output_gz_jsonl_records.json", ALL_SUPPORTED_FOLDER_PATH)) as json_file:
            expected_gz_has_jsonl_records = simplejson.load(json_file, use_decimal = True).get("records", [])
        with open(utils.get_resources_path("output_zip_records.json", ALL_SUPPORTED_FOLDER_PATH)) as json_file:
            expected_zip_records = simplejson.load(json_file, use_decimal = True).get("records", [])

        synced_records = runner.get_records_from_target_output()
        
        csv_upsert_messages = [m for m in synced_records.get('all_support_csv').get('messages') if m['action'] == 'upsert']
        jsonl_upsert_messages = [m for m in synced_records.get('all_support_jsonl').get('messages') if m['action'] == 'upsert']
        gz_with_csv_upsert_messages = [m for m in synced_records.get('all_support_gz_has_csv').get('messages') if m['action'] == 'upsert']
        gz_with_jsonl_upsert_messages = [m for m in synced_records.get('all_support_gz_has_jsonl').get('messages') if m['action'] == 'upsert']
        zip_upsert_messages = [m for m in synced_records.get('all_support_zip').get('messages') if m['action'] == 'upsert']
        
        csv_records = [message.get('data') for message in csv_upsert_messages]
        jsonl_records = [message.get('data') for message in jsonl_upsert_messages]
        gz_has_csv_records = [message.get('data') for message in gz_with_csv_upsert_messages]
        gz_has_jsonl_records = [message.get('data') for message in gz_with_jsonl_upsert_messages]
        zip_records = [message.get('data') for message in zip_upsert_messages]

        no_records = len(csv_records) + len(jsonl_records) + len(gz_has_csv_records) + len(gz_has_jsonl_records) + len(zip_records)
        self.assertEqual(expected_records, no_records)

        self.assertEqual(expected_csv_records, csv_records)
        self.assertEqual(expected_jsonl_records, jsonl_records)
        self.assertEqual(expected_gz_has_csv_records, gz_has_csv_records)
        self.assertEqual(expected_gz_has_jsonl_records, gz_has_jsonl_records)
        self.assertEqual(expected_zip_records, zip_records)

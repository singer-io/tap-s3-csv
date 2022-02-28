import simplejson
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import utils_for_test as utils
import unittest

ALL_SUPPORTED_FOLDER_PATH = "All-Supported-Files"

class S3AllFilesSupport(unittest.TestCase):

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return ["sample_csv_file.csv","sample_jsonl_file.jsonl","sample_gz_file_has_csv.gz","sample_gz_file_has_jsonl.gz","sample_zip_file.zip"]

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

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
        return {}

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            "bucket": "com-stitchdata-prod-circleci-assets",
            "account_id": "218546966473",
            'tables' : "[{\"table_name\": \"all_support_csv\",\"search_prefix\": \"int_test_all_support_csv\",\"search_pattern\": \"int_test_all_support_csv\\\\/.*\\\\.csv\"},{\"table_name\": \"all_support_jsonl\",\"search_prefix\": \"int_test_all_support_jsonl\",\"search_pattern\": \"int_test_all_support_jsonl\\\\/.*\\\\.jsonl\"},{\"table_name\": \"all_support_gz_has_csv\",\"search_prefix\": \"int_test_all_support_gz_has_csv\",\"search_pattern\": \"int_test_all_support_gz_has_csv\\\\/.*\\\\.gz\"},{\"table_name\":\"all_support_gz_has_jsonl\",\"search_prefix\": \"int_test_all_support_gz_has_jsonl\",\"search_pattern\": \"int_test_all_support_gz_has_jsonl\\\\/.*\\\\.gz\"},{\"table_name\": \"all_support_zip\",\"search_prefix\": \"int_test_all_support_zip\",\"search_pattern\": \"int_test_all_support_zip\\\\/.*\\\\.zip\"}]"
        }


    def select_found_catalogs(self, found_catalogs):
        # selected = [menagerie.select_catalog(self.conn_id, c) for c in found_catalogs]
        # menagerie.post_annotated_catalogs(self.conn_id, selected)
        for catalog in found_catalogs:
            schema = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
            non_selected_properties = []
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                self.conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )


    def setUpTestEnvironment(self):
        index = 0
        for resource in self.resource_names():
            utils.delete_and_push_file(self.get_properties(), [resource], ALL_SUPPORTED_FOLDER_PATH, index)
            index += 1


    def test_run(self):

        self.setUpTestEnvironment()

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), len(self.expected_check_streams()), msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_found_catalogs(found_catalogs)

        runner.run_sync_job_and_check_status(self)

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

        self.assertEquals(expected_csv_records, csv_records)
        self.assertEquals(expected_jsonl_records, jsonl_records)
        self.assertEquals(expected_gz_has_csv_records, gz_has_csv_records)
        self.assertEquals(expected_gz_has_jsonl_records, gz_has_jsonl_records)
        self.assertEquals(expected_zip_records, zip_records)

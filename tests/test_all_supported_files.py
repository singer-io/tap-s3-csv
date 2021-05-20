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
            'start_date' : '2017-01-01 00:00:00',
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

        csv_records = 998
        jsonl_records = 10
        gz_has_csv_records = 998
        gz_has_jsonl_records = 2
        zip_records = 40

        expected_records = csv_records + jsonl_records + gz_has_csv_records + gz_has_jsonl_records + zip_records
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

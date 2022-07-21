from base_for_compressed_file import (S3CompressedFile, JSONL_FOLDER_PATH)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest


class S3CompressedZipFile(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'zip_data_has_jsonl_file', 'search_prefix': 'compressed_files_zip_data_has_jsonl_file', 'search_pattern': 'compressed_files_zip_data_has_jsonl_file\\/.*\\.zip'}]

    def resource_names(self):
        return ["sample_compressed_zip_contains_jsonl_file.zip"]

    def name(self):
        return "test_zip_contains_jsonl_file"

    def expected_check_streams(self):
        return {
            'zip_data_has_jsonl_file'
        }

    def expected_sync_streams(self):
        return {
            'zip_data_has_jsonl_file'
        }

    def test_run(self):

        self.setUpTestEnvironment(JSONL_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "zip_data_has_jsonl_file")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 2
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

from base_for_compressed_file import (S3CompressedFile, COMPRESSION_FOLDER_PATH)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest


class S3CompressedZipFileContainsJSONLWithCSV(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'zip_has_jsonl_with_csv', 'search_prefix': 'compressed_files_zip_has_jsonl_with_csv', 'search_pattern': 'compressed_files_zip_has_jsonl_with_csv\\/.*\\.zip'}]

    def resource_names(self):
        return ["jsonl_csv.zip"]

    def name(self):
        return "test_zip_file"

    def expected_check_streams(self):
        return {
            'zip_has_jsonl_with_csv'
        }

    def expected_sync_streams(self):
        return {
            'zip_has_jsonl_with_csv'
        }

    def expected_pks(self):
        return {
            'zip_has_jsonl_with_csv': {}
        }

    def test_run(self):

        self.setUpTestEnvironment(COMPRESSION_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "zip_has_jsonl_with_csv")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 35
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

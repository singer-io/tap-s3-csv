from base_for_compressed_file import (COMPRESSION_FOLDER_PATH, S3CompressedFile)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

class S3CompressedGZFileHavingExtensionCSVorJSONL(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'gz_file_with_csv_and_jsonl_extension', 'search_prefix': 'compressed_files_gz_file_with_csv_and_jsonl_extension', 'search_pattern': 'compressed_files_gz_file_with_csv_and_jsonl_extension\\/.*\\.(csv|jsonl)'}]

    def resource_names(self):
        return ["gz_stored_as_csv.csv","gz_stored_as_jsonl.jsonl"]

    def name(self):
        return "test_gz_file_with_csv_and_jsonl_extension"

    def expected_check_streams(self):
        return {
            'gz_file_with_csv_and_jsonl_extension'
        }

    def expected_sync_streams(self):
        return {
            'gz_file_with_csv_and_jsonl_extension'
        }

    def expected_pks(self):
        return {
            'gz_file_with_csv_and_jsonl_extension': {}
        }

    def test_run(self):

        self.setUpTestEnvironment(COMPRESSION_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "gz_file_with_csv_and_jsonl_extension")

        self.run_and_verify_sync(self.conn_id, is_expected_records_zero=True)

        expected_records = 0
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))
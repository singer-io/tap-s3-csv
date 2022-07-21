from base_for_compressed_file import (COMPRESSION_FOLDER_PATH, S3CompressedFile)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

class S3EmptyJsonJsonlFile(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'empty_json_jsonl_file', 'search_prefix': 'jsonl_files_empty_json_jsonl_file', 'search_pattern': 'jsonl_files_empty_json_jsonl_file\\/.*\\.jsonl'}]

    def resource_names(self):
        return ["empty_json.jsonl","multiple_empty_json.jsonl"]

    def name(self):
        return "test_empty_json_jsonl_file"

    def expected_check_streams(self):
        return {
            'empty_json_jsonl_file'
        }

    def expected_sync_streams(self):
        return {
            'empty_json_jsonl_file'
        }

    def test_run(self):

        self.setUpTestEnvironment("tap-s3-csv")

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "empty_json_jsonl_file")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 0
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

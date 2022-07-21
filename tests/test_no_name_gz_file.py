from base_for_compressed_file import (COMPRESSION_FOLDER_PATH, S3CompressedFile)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

class S3CompressedNoNameGZFile(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'no_name_gz_file', 'search_prefix': 'compressed_files_no_name_gz_file', 'search_pattern': 'compressed_files_no_name_gz_file\\/.*\\.gz'}]

    def resource_names(self):
        return ["sample_compressed_no_name.gz"]

    def name(self):
        return "test_no_name_gz_file"

    def expected_check_streams(self):
        return {
            'no_name_gz_file'
        }

    def expected_sync_streams(self):
        return {
            'no_name_gz_file'
        }

    def expected_pks(self):
        return {
            'no_name_gz_file': {}
        }

    def test_run(self):

        self.setUpTestEnvironment(COMPRESSION_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "no_name_gz_file")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 0
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

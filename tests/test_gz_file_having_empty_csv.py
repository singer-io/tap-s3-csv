from base_for_compressed_file import (COMPRESSION_FOLDER_PATH, S3CompressedFile)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

class S3CompressedGZFileHavingEmptyCSV(S3CompressedFile, S3CSVBaseTest):

    table_entry = [{'table_name': 'gz_file_having_empty_csv', 'search_prefix': 'compressed_files_gz_file_having_empty_csv', 'search_pattern': 'compressed_files_gz_file_having_empty_csv\\/.*\\.(gz)'}]

    def resource_names(self):
        return ["empty_csv_gz.gz"]

    def name(self):
        return "test_gz_file_having_empty_csv"

    def expected_check_streams(self):
        return {
            'gz_file_having_empty_csv'
        }

    def expected_sync_streams(self):
        return {
            'gz_file_having_empty_csv'
        }

    def test_run(self):

        self.setUpTestEnvironment(COMPRESSION_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "gz_file_having_empty_csv")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 0
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest, COMPRESSION_FOLDER_PATH

class S3CompressedGZFileHavingEmptyCSV(S3CSVBaseTest):

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

    def expected_pks(self):
        return {
            'gz_file_having_empty_csv': {}
        }

    def test_run(self):

        self.setUpCompressedEnv(COMPRESSION_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        self.run_and_verify_sync(self.conn_id, is_expected_records_zero=True)

        expected_records = 0
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest, CSV_FOLDER_PATH


class S3CompressedFileNonCSV(S3CSVBaseTest):

    table_entry = [{'table_name': 'zip_with_csv_files', 'search_prefix': 'compressed_files_zip_with_csv_files', 'search_pattern': 'compressed_files_zip_with_csv_files\\/.*'}]

    def resource_names(self):
        return ["csv_files.zip","sample_csv_file_01.csv","sample_csv_file_02.csv"]

    def name(self):
        return "test_zip_with_csv_files"

    def expected_check_streams(self):
        return {
            'zip_with_csv_files'
        }

    def expected_sync_streams(self):
        return {
            'zip_with_csv_files'
        }

    def expected_pks(self):
        return {
            'zip_with_csv_files': {}
        }

    def test_run(self):

        self.setUpCompressedEnv(CSV_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        self.run_and_verify_sync(self.conn_id)

        expected_records = 1983 + 1983
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

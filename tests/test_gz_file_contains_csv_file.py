from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest, CSV_FOLDER_PATH

class S3CompressedGZFile(S3CSVBaseTest):

    table_entry = [{'table_name': 'gz_has_csv_data', 'search_prefix': 'compressed_files_gz_has_csv_data', 'search_pattern': 'compressed_files_gz_has_csv_data\\/.*\\.gz'}]

    def resource_names(self):
        return ["sample_compressed_gz_file.gz"]

    def name(self):
        return "test_gz_file_contains_csv_file"

    def expected_check_streams(self):
        return {
            'gz_has_csv_data'
        }

    def expected_sync_streams(self):
        return {
            'gz_has_csv_data'
        }

    def expected_pks(self):
        return {
            'gz_has_csv_data': {}
        }

    def test_run(self):

        self.setUpCompressedEnv(CSV_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        self.run_and_verify_sync(self.conn_id)

        expected_records = 998
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

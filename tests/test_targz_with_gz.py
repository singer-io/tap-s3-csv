from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest, CSV_FOLDER_PATH

class S3CompressedGZTarGZFile(S3CSVBaseTest):

    table_entry = [{'table_name': 'gz_with_targz_file', 'search_prefix': 'compressed_files_gz_with_targz_file', 'search_pattern': 'compressed_files_gz_with_targz_file\\/.*\\.gz'}]

    def resource_names(self):
        return ["sample_compressed_gz_file.gz","sample_compressed_tar_file.tar.gz"]

    def name(self):
        return "test_targz_not_considered_with_gz"

    def expected_check_streams(self):
        return {
            'gz_with_targz_file'
        }

    def expected_sync_streams(self):
        return {
            'gz_with_targz_file'
        }

    def expected_pks(self):
        return {
            'gz_with_targz_file': {}
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

from base_for_compressed_file import (S3CompressedFile, CSV_FOLDER_PATH)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest

class S3CompressedGZTarGZFile(S3CompressedFile, S3CSVBaseTest):

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

    def test_run(self):

        self.setUpTestEnvironment(CSV_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "gz_with_targz_file")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 998
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

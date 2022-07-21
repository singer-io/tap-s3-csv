from base_for_compressed_file import (S3CompressedFile, CSV_FOLDER_PATH)
from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest


class S3CompressedFileNonCSV(S3CompressedFile, S3CSVBaseTest):

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

        self.setUpTestEnvironment(CSV_FOLDER_PATH)

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "zip_with_csv_files")

        self.run_and_verify_sync(self.conn_id)

        expected_records = 1983 + 1983
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

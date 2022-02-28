from base_for_compressed_file import (S3CompressedFile, CSV_FOLDER_PATH)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

class S3CompressedGZTarGZFile(S3CompressedFile):

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

    def get_properties(self):
        properties = super().get_properties()
        properties["tables"] = "[{\"table_name\": \"gz_with_targz_file\",\"search_prefix\": \"compressed_files_gz_with_targz_file\",\"search_pattern\": \"compressed_files_gz_with_targz_file\\\\/.*\\\\.gz\"}]"
        return properties


    def test_run(self):

        self.setUpTestEnvironment(CSV_FOLDER_PATH)

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "gz_with_targz_file")

        runner.run_sync_job_and_check_status(self)

        expected_records = 998
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

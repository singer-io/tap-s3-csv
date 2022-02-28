from base_for_compressed_file import (S3CompressedFile, JSONL_FOLDER_PATH)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

class S3CompressedGZFile(S3CompressedFile):

    def resource_names(self):
        return ["sample_compressed_gz_file_with_json_file_2_records.gz"]

    def name(self):
        return "test_gz_file_contains_jsonl_file"

    def expected_check_streams(self):
        return {
            'gz_has_jsonl_data'
        }

    def expected_sync_streams(self):
        return {
            'gz_has_jsonl_data'
        }

    def get_properties(self):
        properties = super().get_properties()
        properties["tables"] = "[{\"table_name\": \"gz_has_jsonl_data\",\"search_prefix\": \"compressed_files_gz_has_jsonl_data\",\"search_pattern\": \"compressed_files_gz_has_jsonl_data\\\\/.*\\\\.gz\"}]"
        return properties


    def test_run(self):

        self.setUpTestEnvironment(JSONL_FOLDER_PATH)

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        self.select_specific_catalog(found_catalogs, "gz_has_jsonl_data")

        runner.run_sync_job_and_check_status(self)

        expected_records = 2
        # Verify actual rows were synced
        records  = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records))

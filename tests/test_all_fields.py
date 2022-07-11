from tap_tester import connections, menagerie, runner
import unittest
from utils_for_test import delete_and_push_file

class S3CSVAllFieldsTest(unittest.TestCase):

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), 'PrimaryKey-CSV')
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["CSV_file_with_duplicate_header.csv"]

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return "test_all_fields"

    def expected_check_streams(self):
        return {"all_fields_csv"}

    def expected_automatic_fields(self):
        return {"all_fields_csv": {"head1"}}

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            "start_date" : "2021-11-02T00:00:00Z",
            "bucket": "com-stitchdata-prod-circleci-assets",
            "account_id": "218546966473",
            "tables": "[{\"table_name\": \"all_fields_csv\",\"search_prefix\": \"tap_tester\",\"search_pattern\": \"CSV_file_with_duplicate_header.csv\",\"key_properties\": \"head1\"}]"
        }

    def test_run(self):
        runner.run_check_job_and_check_status(self)

        expected_streams = self.expected_check_streams()
        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        subset = expected_streams.issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in expected_streams]

        stream_to_all_catalog_fields = dict()
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in c_annotated['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[c['stream_name']] = set(fields_from_field_level_md)
        
        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]

                messages = synced_records.get(stream)
                # collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                self.assertSetEqual(expected_all_keys, actual_all_keys)

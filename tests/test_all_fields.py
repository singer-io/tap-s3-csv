from tap_tester import connections, menagerie, runner
from base import S3CSVBaseTest
from utils_for_test import delete_and_push_file

class S3CSVAllFieldsTest(S3CSVBaseTest):

    table_entry = [{'table_name': 'all_fields_csv', 'search_prefix': 'tap_tester', 'search_pattern': 'CSV_file_with_duplicate_header.csv', 'key_properties': 'head1'}]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), 'PrimaryKey-CSV')
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["CSV_file_with_duplicate_header.csv"]

    def name(self):
        return "test_all_fields"

    def expected_check_streams(self):
        return {"all_fields_csv"}

    def expected_sync_streams(self):
        return {"all_fields_csv"}

    def expected_pks(self):
        return {"all_fields_csv": {"head1"}}

    def expected_automatic_fields(self):
        return {"all_fields_csv": {"head1"}}

    def test_run(self):

        expected_streams = self.expected_check_streams()
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

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
        self.run_and_verify_sync(self.conn_id)

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

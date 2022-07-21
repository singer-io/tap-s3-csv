import unittest
from tap_tester import menagerie, runner, connections
from datetime import datetime as dt

from base import S3CSVBaseTest

class S3StartDateTest(S3CSVBaseTest):

    table_entry = [{'table_name': 'employee_table', 'search_prefix': 'tap_tester', 'search_pattern': 'start_date_.*.csv'}]

    def name(self):
        return "test_start_date"

    def expected_check_streams(self):
        return {'employee_table'}

    def expected_sync_streams(self):
        return {'employee_table'}

    def expected_pks(self):
        return {'employee_table': {}}

    def parse_date(self, value, format):
        return dt.strptime(value, format)

    def test_run(self):
        # NOTE: The two test files "start_date_1.csv" and "start_date_2.csv" are
        # added in different dates, and expecting it never gets changed or modified

        self.START_DATE = '2022-07-07T00:00:00Z'

        ############ First sync ############
        self.conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        records_1 = runner.get_records_from_target_output()
        actual_records_1 = [record.get("data") for record in records_1.get("employee_table").get("messages")]
        state_1 = menagerie.get_state(self.conn_id)

        ############ Second sync ############
        self.conn_id = connections.ensure_connection(self, original_properties=False)
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        records_2 = runner.get_records_from_target_output()
        actual_records_2 = [record.get("data") for record in records_2.get("employee_table").get("messages")]
        state_2 = menagerie.get_state(self.conn_id)

        # Verify we synced more records in 2nd sync in comparision to 1st sync, as file is updated
        self.assertGreater(len(actual_records_1), len(actual_records_2))

        # Verify we get same bookmark for both syncs
        self.assertEqual(
            self.parse_date(state_2.get("bookmarks").get("employee_table").get("modified_since"), "%Y-%m-%dT%H:%M:%S+00:00"),
            self.parse_date(state_1.get("bookmarks").get("employee_table").get("modified_since"), "%Y-%m-%dT%H:%M:%S+00:00"))

        # Verify we replicated sync 2 records in 1st sync too
        self.assertTrue(set(records_2).issubset(set(records_1)))

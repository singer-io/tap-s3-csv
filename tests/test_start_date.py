import unittest
from tap_tester import menagerie, runner, connections
from datetime import datetime as dt

class S3StartDateTest(unittest.TestCase):

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return "test_start_date"

    def expected_check_streams(self):
        return {'employee_table'}

    def get_credentials(self):
        return {}

    def get_properties(self, original: bool = True):
        props = {
            'start_date' : '2022-07-06T00:00:00Z',
            'bucket': 'com-stitchdata-prod-circleci-assets',
            'account_id': '218546966473',
            'tables': "[{\"table_name\": \"employee_table\",\"search_prefix\": \"tap_tester\",\"search_pattern\": \"start_date_.*.csv\"}]"
        }
        if original:
            return props

        props["start_date"] = '2022-07-07T00:00:00Z'
        return props

    def parse_date(self, value, format):
        return dt.strptime(value, format)

    def test_run(self):

        ############ First sync ############
        self.conn_id = connections.ensure_connection(self)
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        records_1 = runner.get_records_from_target_output()
        actual_records_1 = [record.get("data") for record in records_1.get("employee_table").get("messages")]
        state_1 = menagerie.get_state(self.conn_id)

        ############ Second sync ############
        self.conn_id = connections.ensure_connection(self, original_properties=False)
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_check_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

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

        # verify we replicated sync 2 records in 1st sync too
        self.assertTrue(set(records_2).issubset(set(records_1)))

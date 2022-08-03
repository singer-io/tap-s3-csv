import json
import time
import unittest
import utils_for_test as utils
from tap_tester import connections, menagerie, runner
from datetime import datetime as dt

CSV_FOLDER_PATH = "Compressed-CSV"
COMPRESSION_FOLDER_PATH = "Compressed"
JSONL_FOLDER_PATH = "Compressed-JSONL"

class S3CSVBaseTest(unittest.TestCase):

    table_entry = None
    START_DATE = None

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-s3-csv"

    @staticmethod
    def get_type():
        return "platform.s3-csv"

    @staticmethod
    def get_credentials():
        return {}

    def setUpCompressedEnv(self, folder_path):
        self.conn_id = connections.ensure_connection(self)
        utils.delete_and_push_file(self.get_properties(), self.resource_names(), folder_path)

    def get_properties(self, original: bool = True):
        props = {
            'start_date' : '2021-11-02T00:00:00Z',
            'bucket': 'com-stitchdata-prod-circleci-assets',
            'account_id': '218546966473',
            'tables': json.dumps(self.table_entry)
        }
        if original:
            return props

        props['start_date'] = self.START_DATE
        return props

    def expected_pks(self):
        return {}

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be run prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, is_expected_records_zero=False):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_check_streams(),
                                                              self.expected_pks())

        if not is_expected_records_zero:
            self.assertGreater(
                sum(sync_record_count.values()), 0,
                msg="failed to replicate any data: {}".format(sync_record_count)
            )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def get_selected_fields_from_metadata(self,metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)
        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def dt_to_ts(dtime , format):
        """Converts date to time-stamp"""
        return int(time.mktime(dt.strptime(dtime, format).timetuple()))

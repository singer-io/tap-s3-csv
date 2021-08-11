import json
import boto3
import unittest
import utils_for_test as utils
import os

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

TAP_S3_CSV_PATH = "tap-s3-csv"

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)


class S3CatalogWithoutProperties(unittest.TestCase):

    def resource_names(self):
        return ["test_empty_catalog_1.csv", "test_empty_catalog_2.csv", "test_empty_catalog_3.csv", "test_empty_catalog_4.csv", "test_empty_catalog_5.csv", "test_empty_catalog_6.csv", "test_empty_catalog_7.csv"]

    def tap_name(self):
        return "tap-s3-csv"

    def name(self):
        return "tap_tester_s3_catalog_without_properties_csv"

    def get_type(self):
        return "platform.s3-csv"

    def get_credentials(self):
        return {}

    def expected_streams(self):
        return {
            'catalog_without_properties'
        }

    def expected_pks(self):
        return {}

    def get_properties(self):
        with open(get_resources_path("tap-s3-csv/catalog_without_properties_case.json"), encoding='utf-8') as file:
            data = json.load(file)
            data["tables"] = json.dumps(data["tables"])

        return data

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def setUpTestEnvironment(self):
        for resource in self.resource_names():
            utils.delete_and_push_file(self.get_properties(), [
                                       resource], TAP_S3_CSV_PATH)

    def test_catalog_without_properties(self):

        self.setUpTestEnvironment()

        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1,
                         msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(
            map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_streams().issubset(found_catalog_names)
        self.assertTrue(
            subset, msg="Expected check streams are not subset of discovered catalog")

        our_catalogs = [c for c in found_catalogs if c.get(
            'tap_stream_id') in self.expected_streams()]

        # Select our catalogs
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(
                self.conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                self.conn_id, c, c_annotated, [], [])

        #Verify that schema contains empty properties
        expected_schema = {
            'type': 'object',
            'properties': {}
        }
        self.assertEqual(expected_schema, c_annotated.get('annotated-schema', {}))

        # Stream properties should be zero as all 5 files considered in sampling are containing headers only.
        # No fields with breadcumb will be present in schema
        metadata = c_annotated["metadata"]
        stream_properties = [item for item in metadata if item.get("breadcrumb") != []]
        self.assertEqual(len(stream_properties), 0)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        synced_records = runner.get_records_from_target_output()
        upsert_messages = [m for m in synced_records.get(
            'catalog_without_properties').get('messages') if m['action'] == 'upsert']

        records = [message.get('data') for message in upsert_messages]

        #All fields from file test_empty_catalog_7.csv should be emitted with duplicate & no header handling
        #as catalog is without any fields.

        expected_records = [
            {'id': '1', 'name': 'John', '_sdc_extra': [{'name': 'carl'}], '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_empty_catalog_7.csv', '_sdc_source_lineno': 2},
            {'id': '2', 'name': 'Bob', '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_empty_catalog_7.csv', '_sdc_source_lineno': 3},
            {'id': '3', '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
                '_sdc_source_file': 'tap_tester/test_empty_catalog_7.csv', '_sdc_source_lineno': 4},
            {'id': '4', 'name': 'Alice', '_sdc_extra': [{'no_headers': ['Ben', '5']}, {
                'name': 'Barak'}], '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets', '_sdc_source_file': 'tap_tester/test_empty_catalog_7.csv', '_sdc_source_lineno': 5}
        ]

        self.assertListEqual(expected_records, records)


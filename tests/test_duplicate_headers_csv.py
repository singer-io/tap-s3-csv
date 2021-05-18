import json
import boto3
import unittest
import os

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner as runner

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)

class S3DuplicateHeadersCSV(unittest.TestCase):

    def tap_name(self):
        return "tap-s3-csv"

    def name(self):
        return "tap_tester_s3_duplicate_headers_csv"

    def get_type(self):
        return "platform.s3-csv"

    def get_credentials(self):
        return {}

    def expected_streams(self):
        return {
            'duplicate_headers'
        }

    def expected_pks(self):
        return {
            'duplicate_headers': {"a0"}
        }

    def get_properties(self):
        with open(get_resources_path("tap-s3-csv/duplicate_headers_csv_config.json"), encoding='utf-8') as file:
            data = json.load(file)
            data["tables"] = json.dumps(data["tables"])

        return data

    def setUp(self):
        s3_client = boto3.resource('s3')
        properties = self.get_properties()

        # Parsing the properties tables
        tables = json.loads(properties['tables'])

        s3_bucket = s3_client.Bucket(properties['bucket'])

        resource_name = "tap-s3-csv/duplicate_headers.csv"
        s3_path = tables[0]['search_prefix'] + '/' + resource_name
        s3_object = s3_bucket.Object(s3_path)

        # Put S3 File to com-stitchdata-prod-circleci-assets/s3_discovery_test
        print("Attempting to upload S3 file {} before test.".format(resource_name))
        s3_object.upload_file(get_resources_path(resource_name))
        self.conn_id = connections.ensure_connection(self)


    def test_duplicate_headers_in_csv(self):
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertEqual(len(found_catalogs), 1, msg="unable to locate schemas for connection {}".format(self.conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        
        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(self.conn_id, c, c_annotated, [], [])

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, self.conn_id, self.expected_streams(), self.expected_pks())
        self.assertGreater(sum(record_count_by_stream.values()), 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(sum(record_count_by_stream.values())))

        synced_records = runner.get_records_from_target_output()
        upsert_messages = [m for m in synced_records.get('duplicate_headers').get('messages') if m['action'] == 'upsert']
        
        records = [message.get('data') for message in upsert_messages]
        
        expected_records = [{'a0': 'a1', 'b0': 'b1', 'c0': 'c1', 'd0': 'd1', 'e0': 'e1', 'f0': 'f1', '_sdc_extra': ['{"a0": "a11"}', '{"b0": ["b11", "b12", "b13"]}', '{"c0": "c11"}'], '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 2}, {'a0': 'a2', 'b0': 'b2', 'c0': 'c2', 'd0': 'd2', 'e0': 'e2', 'f0': 'f2', '_sdc_extra': ['{"a0": "a21"}', '{"b0": "b21"}'], '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 3}, {'a0': 'a3', 'b0': 'b3', 'c0': 'c3', '_sdc_extra': ['{"a0": "a31"}'], '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 4}, {'a0': 'a4', '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 5}, {'a0': 'a5', 'b0': '', 'c0': 'c5', 'd0': 'd5', '_sdc_extra': ['{"a0": ""}'], '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 6}, {'a0': 'a6', 'b0': 'b6', 'c0': 'c6', 'd0': 'd6', 'e0': 'e6', 'f0': 'f6', '_sdc_extra': ['{"no_headers": ["g0", "h0", "i0"]}', '{"a0": "a61"}', '{"b0": ["b61", "b62", "b63"]}', '{"c0": "c61"}'], '_sdc_source_bucket': 'qradars3test', '_sdc_source_file': 'tap_tester/tap-s3-csv/duplicate_headers.csv', '_sdc_source_lineno': 7}]

        self.assertListEqual(expected_records, records)


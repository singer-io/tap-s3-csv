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

    def expected_stream_row_counts(self):
        row_count = 0
        resource_name = "tap-s3-csv/duplicate_headers.csv"
        with open(get_resources_path(resource_name), 'r', encoding='utf-8') as f:
            for i, l in enumerate(f):
                pass
            row_count += i

        return row_count

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

        # verify that when duplicate headers are present, the _sdc_extra has the values
        sdc_extra = [message for message in upsert_messages
                    if len(message.get('data').get('_sdc_extra', [])) == 5]

        self.assertEqual(len(sdc_extra), self.expected_stream_row_counts() - 1)

        #Verify that when duplicate headers are present and extra values present, the _sdc_exra has the values
        expected_sdc_extra_value = ['v9','w9','x9','y9','z9','aa0','aa1','aa2']
        sdc_extra1 = [message.get('data').get('_sdc_extra') for message in upsert_messages
                    if len(message.get('data').get('_sdc_extra', [])) == 8]

        self.assertEqual(len(sdc_extra1), 1)
        self.assertEqual(sdc_extra1[0], expected_sdc_extra_value)

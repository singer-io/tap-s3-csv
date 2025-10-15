import json
import unittest
from base import S3CSVBaseTest
from tap_tester import connections, menagerie, runner

class AvroSyncFileTest(S3CSVBaseTest):

    table_entry = [{'table_name': 'avro', 'search_prefix': 'tap-s3-csv', 'search_pattern': 'nestedfile.avro'}]

    def resource_name(self):
        return ["nestedfile.avro"]

    def name(self):
        return "test_avro"

    def expected_check_streams(self):
        return {"avro"}

    def expected_sync_streams(self):
        return {"avro"}

    def expected_pks(self):
        return {"avro": set()}

    def expected_automatic_fields(self):
        return {"avro": set()}

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        c = menagerie.get_catalogs(conn_id)[0]
        c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
        self.perform_and_verify_table_and_field_selection(conn_id, [c], True)

        expected_schema = {
            'type': ['null', 'object'],
            'properties': {
                'id': {'type': ['null', 'integer'], 'inclusion': 'available'},
                'nested': {
                    'type': ['null', 'object'],
                    'properties': {
                        'name': {'type': ['null', 'string']},
                        'bytes': {'type': ['null', 'string']}
                    },
                    'inclusion': 'available'
                },
                '_sdc_source_bucket': {'type': 'string', 'inclusion': 'available'},
                '_sdc_source_file': {'type': 'string', 'inclusion': 'available'},
                '_sdc_source_lineno': {'type': 'integer', 'inclusion': 'available'},
                '_sdc_extra': {
                    'type': 'array',
                    'items': {
                        'anyOf': [{'type': 'object', 'properties': {}}, {'type': 'string'}]
                    },
                'inclusion': 'available'}
            }
        }

        self.assertEqual(expected_schema, c_annotated.get('annotated-schema'))

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        records = runner.get_records_from_target_output()
        actual_records = [record.get("data") for record in records.get("avro").get("messages")]
        expected_records = [
            {'id': n,
             'nested': {'name': f'name_{n}', 'bytes': "b'abcdefg'"},
             '_sdc_source_bucket': 'com-stitchdata-prod-circleci-assets',
             '_sdc_source_file': 'tap-s3-csv/nestedfile.avro',
             '_sdc_source_lineno': n + 1}
            for n in range(10000)]

        self.assertEqual(expected_records, actual_records)

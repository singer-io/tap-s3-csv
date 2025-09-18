import json
import unittest
from base import S3CSVBaseTest
from tap_tester import connections, menagerie, runner

class ParquetSyncFileTest(S3CSVBaseTest):

    table_entry = [{'table_name': 'sample_parquet', 'search_prefix': '', 'search_pattern': 'parquetfile1.parquet'}]

    def resource_name(self):
        return ["parquetfile1.parquet"]

    def name(self):
        return "test_parquet"

    def expected_check_streams(self):
        return {"parquet"}

    def expected_sync_streams(self):
        return {"parquet"}

    def expected_pks(self):
        return {"parquet": set()}

    def expected_automatic_fields(self):
        return {"parquet": set()}

    def get_properties(self, original: bool = True):
        props = {
            'start_date' : '2021-11-02T00:00:00Z',
            'bucket': 'tap-s3-csv-test-bucket',
            'account_id': '218546966473',
            'tables': json.dumps(self.table_entry)
        }
        if original:
            return props

        props['start_date'] = self.START_DATE
        return props

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        c = menagerie.get_catalogs(conn_id)[0]
        c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])
        print(c_annotated)

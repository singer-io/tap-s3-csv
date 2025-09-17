import unittest
from tap_tester import connections, menagerie, runner

class ParquetSyncFileTest(unittest.TestCase):
    def test_run(self):
        props = {
            'start_date' : '2021-11-02T00:00:00Z',
            'bucket': 'tap-s3-csv-test-bucket',
            'account_id': '218546966473',
            'tables': [{
                'table_name': 'sample_parquet',
                'search_prefix': '',
                'search_pattern': 'parquetfile1.parquet'
            }]
        }

        conn_id = connections.ensure_connection(self)
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        assertEqual({}, menagerie.get_catalogs(conn_id))

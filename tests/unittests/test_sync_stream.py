import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from tap_s3_csv import sync_stream

class TestSyncStream(unittest.TestCase):

    @patch('tap_s3_csv.s3.get_input_files_for_table')
    @patch('tap_s3_csv.sync.sync_table_file')
    @patch('tap_s3_csv.singer.get_bookmark')
    @patch('tap_s3_csv.singer.write_bookmark')
    @patch('tap_s3_csv.singer.write_state')
    @patch('tap_s3_csv.LOGGER')
    def test_sync_stream_with_files(self, mock_logger, mock_write_state, mock_write_bookmark, mock_get_bookmark, mock_sync_table_file, mock_get_input_files_for_table):
        """
        Tests the sync_stream function with various file modification times.
        Depending on whether the last_modified date is earlier or later than sync_start_time,
        the bookmark will either be updated to the file's last_modified or the sync_start_time.
        """
        test_cases = [
            # Case when file is older than sync_start_time
            {
                "file_last_modified": datetime(2024, 8, 13, 12, 0, 0),
                "sync_start_time": datetime(2024, 8, 14, 12, 0, 0),
                "expected_bookmark": '2024-08-13T12:00:00',
                "expected_records_streamed": 1
            },
            # Case when file is newer than sync_start_time
            {
                "file_last_modified": datetime(2024, 8, 15, 12, 0, 0),
                "sync_start_time": datetime(2024, 8, 14, 12, 0, 0),
                "expected_bookmark": '2024-08-14T12:00:00',
                "expected_records_streamed": 1
            },
                        # Case when file is newer than sync_start_time
            {
                "file_last_modified": datetime(2024, 8, 14, 12, 0, 0),
                "sync_start_time": datetime(2024, 8, 14, 12, 0, 0),
                "expected_bookmark": '2024-08-14T12:00:00',
                "expected_records_streamed": 1
            }
        ]

        mock_get_bookmark.return_value = '2024-01-01T00:00:00Z'
        mock_sync_table_file.return_value = 1
        mock_write_state.return_value = None

        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}
        table_spec = {'table_name': 'test_table'}
        stream = None

        for case in test_cases:
            with self.subTest(case=case):
                mock_get_input_files_for_table.return_value = [{'key': 'file1.csv', 'last_modified': case["file_last_modified"]}]
                mock_write_bookmark.return_value = case["expected_bookmark"]

                records_streamed = sync_stream(config, state, table_spec, stream, case["sync_start_time"])

                self.assertEqual(records_streamed, case["expected_records_streamed"])
                mock_write_bookmark.assert_called_with(state, 'test_table', 'modified_since', case["expected_bookmark"])

                # Ensure `write_state` is called exactly once for each test case
                mock_write_state.assert_called_once()
                mock_write_state.reset_mock()  # Reset the mock call count for the next subtest

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from tap_s3_csv import sync_stream
from parameterized import parameterized

class TestSyncStream(unittest.TestCase):

    @parameterized.expand([
        # Case when file is older than sync_start_time
        ("file_older_than_sync_start_time", datetime(2024, 8, 13, 12, 0, 0), datetime(2024, 8, 14, 12, 0, 0), '2024-08-13T12:00:00', 1),
        # Case when file is newer than sync_start_time
        ("file_newer_than_sync_start_time", datetime(2024, 8, 15, 12, 0, 0), datetime(2024, 8, 14, 12, 0, 0), '2024-08-14T12:00:00', 1),
        # Case when file is the same as sync_start_time
        ("file_same_as_sync_start_time", datetime(2024, 8, 14, 12, 0, 0), datetime(2024, 8, 14, 12, 0, 0), '2024-08-14T12:00:00', 1)
    ])
    @patch('tap_s3_csv.s3.get_input_files_for_table')
    @patch('tap_s3_csv.sync.sync_table_file')
    @patch('tap_s3_csv.singer.get_bookmark')
    @patch('tap_s3_csv.singer.write_bookmark')
    @patch('tap_s3_csv.singer.write_state')
    @patch('tap_s3_csv.LOGGER')
    def test_sync_stream(self, name, file_last_modified, sync_start_time, expected_bookmark, expected_records_streamed, mock_logger, mock_write_state, mock_write_bookmark, mock_get_bookmark, mock_sync_table_file, mock_get_input_files_for_table):
        """
        Parameterized test for the sync_stream function with various file modification times.
        """
        mock_get_bookmark.return_value = '2024-01-01T00:00:00Z'
        mock_sync_table_file.return_value = 1
        mock_write_state.return_value = None

        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}
        table_spec = {'table_name': 'test_table'}
        stream = None

        mock_get_input_files_for_table.return_value = [{'key': 'file1.csv', 'last_modified': file_last_modified}]
        mock_write_bookmark.return_value = expected_bookmark

        records_streamed = sync_stream(config, state, table_spec, stream, sync_start_time)

        self.assertEqual(records_streamed, expected_records_streamed)
        mock_write_bookmark.assert_called_with(state, 'test_table', 'modified_since', expected_bookmark)
        mock_write_state.assert_called_once()
        mock_write_state.reset_mock()

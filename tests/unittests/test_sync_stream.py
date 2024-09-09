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
    def test_sync_stream_with_files_older_than_sync_start_time(self, mock_logger, mock_write_state, mock_write_bookmark, mock_get_bookmark, mock_sync_table_file, mock_get_input_files_for_table):
        """
        Tests the sync_stream function when the last_modified date of files is earlier than sync_start_time.
        In this case, the bookmark is updated to the last_modified date of the file.
        """
        mock_get_bookmark.return_value = '2024-01-01T00:00:00Z'
        mock_get_input_files_for_table.return_value = [
            {'key': 'file1.csv', 'last_modified': datetime(2024, 8, 13, 12, 0, 0)}
        ]
        mock_sync_table_file.return_value = 1
        mock_write_bookmark.return_value = '2024-08-13T12:00:00Z'

        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}
        table_spec = {'table_name': 'test_table'}
        stream = None
        sync_start_time = datetime(2024, 8, 14, 12, 0, 0)

        records_streamed = sync_stream(config, state, table_spec, stream, sync_start_time)

        self.assertEqual(records_streamed, 1)
        # Verify that the bookmark was updated to the last_modified date of the file
        mock_write_bookmark.assert_called_with(state, 'test_table', 'modified_since', '2024-08-13T12:00:00')
        mock_write_state.assert_called_once()

    @patch('tap_s3_csv.s3.get_input_files_for_table')
    @patch('tap_s3_csv.sync.sync_table_file')
    @patch('tap_s3_csv.singer.get_bookmark')
    @patch('tap_s3_csv.singer.write_bookmark')
    @patch('tap_s3_csv.singer.write_state')
    @patch('tap_s3_csv.LOGGER')
    def test_sync_stream_with_files_newer_than_sync_start_time(self, mock_logger, mock_write_state, mock_write_bookmark, mock_get_bookmark, mock_sync_table_file, mock_get_input_files_for_table):
        """
        Tests the sync_stream function when the last_modified date of files is later than sync_start_time.
        In this case, the bookmark is updated to sync_start_time.
        """
        mock_get_bookmark.return_value = '2024-01-01T00:00:00Z'
        mock_get_input_files_for_table.return_value = [
            {'key': 'file1.csv', 'last_modified': datetime(2024, 8, 15, 12, 0, 0)}
        ]
        mock_sync_table_file.return_value = 1
        mock_write_bookmark.return_value = '2024-08-15T12:00:00Z'

        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}
        table_spec = {'table_name': 'test_table'}
        stream = None
        sync_start_time = datetime(2024, 8, 14, 12, 0, 0)

        records_streamed = sync_stream(config, state, table_spec, stream, sync_start_time)

        self.assertEqual(records_streamed, 1)
        # Verify that the bookmark was updated to the sync_start_time
        mock_write_bookmark.assert_called_with(state, 'test_table', 'modified_since', sync_start_time.isoformat())
        mock_write_state.assert_called_once()

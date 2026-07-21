import unittest
from unittest import mock
from tap_s3_csv import s3


def mock_sample_file_success(table_spec, s3_path, file_handle, sample_rate, extension, max_records=1000):
    yield {'id': 1}


def mock_sample_file_raises_unicode_error(table_spec, s3_path, file_handle, sample_rate, extension, max_records=1000):
    raise UnicodeDecodeError('utf-8', b'', 0, 1, 'bad byte')


class TestSampleFilesClosesFileHandles(unittest.TestCase):
    """
    sample_files() should always close each file's handle once it is done
    sampling that file, regardless of whether sampling succeeded or raised
    a (handled) parsing error. get_files_to_sample() returns every handle in
    a single list that is kept alive for the whole generator's lifetime, so
    relying on reference counting alone does not release handles as each
    file finishes - sample_files() must close them explicitly.
    """

    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_sample_file_success)
    @mock.patch("tap_s3_csv.s3.get_files_to_sample")
    def test_closes_handle_after_successful_sampling(self, mock_get_files_to_sample, mock_sample_file):
        mock_handle_1 = mock.Mock()
        mock_handle_2 = mock.Mock()
        mock_get_files_to_sample.return_value = [
            {"s3_path": "file1.csv", "file_handle": mock_handle_1, "extension": "csv"},
            {"s3_path": "file2.csv", "file_handle": mock_handle_2, "extension": "csv"},
        ]

        list(s3.sample_files({}, {}, [], max_files=2))

        mock_handle_1.close.assert_called_once()
        mock_handle_2.close.assert_called_once()

    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_sample_file_raises_unicode_error)
    @mock.patch("tap_s3_csv.s3.get_files_to_sample")
    def test_closes_handle_even_when_sampling_raises_handled_error(self, mock_get_files_to_sample, mock_sample_file):
        mock_handle = mock.Mock()
        mock_get_files_to_sample.return_value = [
            {"s3_path": "bad_file.csv", "file_handle": mock_handle, "extension": "csv"},
        ]

        list(s3.sample_files({}, {}, [], max_files=1))

        mock_handle.close.assert_called_once()

    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_sample_file_success)
    @mock.patch("tap_s3_csv.s3.get_files_to_sample")
    def test_does_not_raise_when_handle_has_no_close(self, mock_get_files_to_sample, mock_sample_file):
        # file_handle objects without a close() method (e.g. plain values used
        # in some test doubles) should not cause sample_files() to error out.
        mock_get_files_to_sample.return_value = [
            {"s3_path": "file1.csv", "file_handle": object(), "extension": "csv"},
        ]

        # Should not raise
        result = list(s3.sample_files({}, {}, [], max_files=1))
        self.assertEqual(result, [{'id': 1}])

    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_sample_file_success)
    @mock.patch("tap_s3_csv.s3.get_files_to_sample")
    def test_does_not_raise_when_close_itself_raises(self, mock_get_files_to_sample, mock_sample_file):
        mock_handle = mock.Mock()
        mock_handle.close.side_effect = OSError("connection already closed")
        mock_get_files_to_sample.return_value = [
            {"s3_path": "file1.csv", "file_handle": mock_handle, "extension": "csv"},
        ]

        # Should not raise even though close() itself errors
        result = list(s3.sample_files({}, {}, [], max_files=1))
        self.assertEqual(result, [{'id': 1}])
        mock_handle.close.assert_called_once()

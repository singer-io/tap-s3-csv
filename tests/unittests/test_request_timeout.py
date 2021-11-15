import unittest
from tap_s3_csv import s3
from unittest import mock

@mock.patch("boto3.resource")
@mock.patch("boto3.client")
@mock.patch("tap_s3_csv.s3.Config")
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"bucket": "test"} # No timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

    def test_integer_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": 100} # integer timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100, read_timeout=100)

        config = {"bucket": "test", "request_timeout": 200} # integer timeout in config
        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=200, read_timeout=200)

    def test_float_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": 100.5} # float timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100.5, read_timeout=100.5)

        config = {"bucket": "test", "request_timeout": 200.5} # float timeout in config
        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=200.5, read_timeout=200.5)

    def test_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": '100'} # string format timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100, read_timeout=100)

        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100, read_timeout=100)

    def test_empty_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {"bucket": "test", "request_timeout": ''} # empty string in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

    def test_zero_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {"bucket": "test", "request_timeout": 0.0} # zero value in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

    def test_zero_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {"bucket": "test", "request_timeout": '0.0'} # zero value in config
        # Call get_file_handle() which set timeout with Config object
        s3.get_file_handle(config, "test")
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

        # Call list_files_in_bucket() which set timeout with Config object
        file_handles = list(s3.list_files_in_bucket(config, "test"))
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)

import unittest
from tap_s3_csv import s3
from unittest import mock
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError
from botocore.paginate import PageIterator

@mock.patch("boto3.resource")
@mock.patch("boto3.client")
@mock.patch("tap_s3_csv.s3.Config")
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"bucket": "test"} # No timeout in config
        s3_client = s3.S3Client(config)
        # Call get_file_handle() which set timeout with Config object
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)
        self.assertEqual(s3_client.get_request_timeout(), 300)

    def test_integer_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": 100} # integer timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100, read_timeout=100)
        self.assertEqual(s3_client.get_request_timeout(), 100)

    def test_float_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": 100.5} # float timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100.5, read_timeout=100.5)
        self.assertEqual(s3_client.get_request_timeout(), 100.5)

    def test_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {"bucket": "test", "request_timeout": '100'} # string format timeout in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=100, read_timeout=100)
        self.assertEqual(s3_client.get_request_timeout(), 100)

    def test_empty_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {"bucket": "test", "request_timeout": ''} # empty string in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)
        self.assertEqual(s3_client.get_request_timeout(), 300)

    def test_zero_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {"bucket": "test", "request_timeout": 0.0} # zero value in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)
        self.assertEqual(s3_client.get_request_timeout(), 300)

    def test_zero_string_request_timeout_in_config(self, mocked_boto_config, mocked_client, mocked_resource):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {"bucket": "test", "request_timeout": '0.0'} # zero value in config
        # Call get_file_handle() which set timeout with Config object
        s3_client = s3.S3Client(config)
        s3_client.get_file_handle({"filepath": "test"})
        # Verify Config is called with expected timeout
        mocked_boto_config.assert_called_with(connect_timeout=300, read_timeout=300)
        self.assertEqual(s3_client.get_request_timeout(), 300)

# Mock objects for boto resource
class MockObjectConnect:
    def get():
        raise ConnectTimeoutError(endpoint_url="test")

class MockBucketConnect:
    def Object(self):
        return MockObjectConnect
    
class MockResourceConnect:
    def Bucket(self):
        return MockBucketConnect

@mock.patch("time.sleep")
class TestConnectTimeoutErrorBackoff(unittest.TestCase):

    @mock.patch("boto3.resource")
    @mock.patch("tap_s3_csv.s3.Config")
    def test_connect_timeout_on_get_file_handle(self, mocked_boto_config, mocked_resource, mocked_sleep):
        """
            Verify that backoff is working properly on get_file_handle() function for ConnectTimeoutError
        """
        # Set config and resource object to raise ConnectTimeoutError
        config = {"bucket": "test"}
        s3_client = s3.S3Client(config)
        mocked_resource.return_value = MockResourceConnect
        try:
            s3_client.get_file_handle({"filepath": "test"})
        except ConnectTimeoutError as e:
            pass
    
        # Verify that resource ans Config object called 5 times
        self.assertEquals(mocked_resource.call_count, 5)
        self.assertEquals(mocked_boto_config.call_count, 5)

    def test_connect_timeout_on_make_request(self, mocked_sleep):
        """
            Verify that backoff is working properly on _make_request() function of botocore for ConnectTimeoutError
        """
        # Mock PageIterator.method to raise ConnectTimeoutError error
        mocked_method = mock.Mock()
        mocked_method.side_effect = ConnectTimeoutError(endpoint_url="test")
        
        try:
            # Initialize PageIterator object and call _make_request function
            paginator = PageIterator(mocked_method, "", "", "", "", "", "", "", "", "", "")
            response = paginator._make_request({})
        except ConnectTimeoutError as e:
            pass
    
        # Verify that PageIterator.method called 5 times
        self.assertEquals(mocked_method.call_count, 5)


# Mock objects for boto resource
class MockObjectRead:
    def get():
        raise ReadTimeoutError(endpoint_url="test")

class MockBucketRead:
    def Object(self):
        return MockObjectRead
    
class MockResourceRead:
    def Bucket(self):
        return MockBucketRead

@mock.patch("time.sleep")
class TestReadTimeoutErrorBackoff(unittest.TestCase):
    
    @mock.patch("boto3.resource")
    @mock.patch("tap_s3_csv.s3.Config")
    def test_read_timeout_on_get_file_handle(self, mocked_boto_config, mocked_resource, mocked_sleep):
        """
            Verify that backoff is working properly on get_file_handle() function for ReadTimeoutError
        """
        # Set config and resource object to raise ReadTimeoutError
        config = {"bucket": "test"}
        s3_client = s3.S3Client(config)
        mocked_resource.return_value = MockResourceRead
        try:
            s3_client.get_file_handle({"filepath": "test"})
        except ReadTimeoutError as e:
            pass

        # Verify that resource ans Config object called 5 times
        self.assertEquals(mocked_resource.call_count, 5)
        self.assertEquals(mocked_boto_config.call_count, 5)

    def test_read_timeout_on_make_request(self, mocked_sleep):
        """
            Verify that backoff is working properly on _make_request() function of botocore for ReadTimeoutError
        """
        # Mock PageIterator.method to raise ReadTimeoutError error
        mocked_method = mock.Mock()
        mocked_method.side_effect = ReadTimeoutError(endpoint_url="test")
        
        try:
            # Initialize PageIterator object and call _make_request function
            paginator = PageIterator(mocked_method, "", "", "", "", "", "", "", "", "", "")
            response = paginator._make_request({})
        except ReadTimeoutError as e:
            pass
    
        # Verify that PageIterator.method called 5 times
        self.assertEquals(mocked_method.call_count, 5)

import unittest
from unittest import mock
from tap_s3_csv.s3 import PageIterator
from botocore.exceptions import ClientError
from parameterized import parameterized

def get(*args, **kwargs):
    """Function to raise an appropriate error as per the arguments"""
    if kwargs.get("access_denied_error"):
        raise ClientError({"Error": {"Message": "Access Denied", "Code": "AccessDenied"}}, "ListObjectsV2")
    elif kwargs.get("AccessDenied_error"):
        raise ClientError({"Error": {"Message": "You arr not authorized to perform this action", "Code": "AccessDenied"}}, "ListObjectsV2")
    else:
        raise ClientError({"Error": {"Message": "Test Error", "Code": "TestError"}}, "ListObjectsV2")

class TestGiveUpForAccessDeniedError(unittest.TestCase):
    """Test case to verify we giveup when we encounter Access Denied error"""

    @parameterized.expand([
        ["giveup_for_access_denied_error", {"access_denied_error": True}, 1],
        ["giveup_for_AccessDenied_error", {"AccessDenied_error": True}, 1],
        ["not_giveup", {}, 5],
    ])
    @mock.patch("time.sleep")
    def test_giveup_for_access_denied_error(self, name, test_data, expected_data, mocked_sleep):

        mocked_method = mock.Mock()
        mocked_method.side_effect = get

        # Create PageIterator object
        page_iter = PageIterator(
            method=mocked_method,
            input_token=None,
            output_token=None,
            more_results=None,
            result_keys=None,
            non_aggregate_keys=None,
            limit_key=None,
            max_items=None,
            starting_token=None,
            page_size=None,
            op_kwargs=None,
        )

        with self.assertRaises(ClientError) as e:
            # Function call
            page_iter._make_request(test_data)

        # Verify the call count
        self.assertEqual(mocked_method.call_count, expected_data)

import unittest
from unittest import mock

import tap_s3_csv


class TestShouldAssumeRole(unittest.TestCase):
    def test_defaults_to_assuming_role(self):
        self.assertTrue(tap_s3_csv.should_assume_role({}))

    def test_assume_role_true(self):
        self.assertTrue(tap_s3_csv.should_assume_role({"assume_role": True}))

    def test_assume_role_false_opts_out(self):
        self.assertFalse(tap_s3_csv.should_assume_role({"assume_role": False}))

    def test_non_false_values_still_assume_role(self):
        # Secure by default: only an explicit boolean False disables assumption.
        self.assertTrue(tap_s3_csv.should_assume_role({"assume_role": "false"}))
        self.assertTrue(tap_s3_csv.should_assume_role({"assume_role": None}))


def _base_args(config):
    args = mock.Mock()
    args.config = config
    args.discover = False
    args.catalog = None
    args.properties = None
    args.state = {}
    return args


@mock.patch("tap_s3_csv.s3")
@mock.patch("tap_s3_csv.validate_table_config", return_value=[])
@mock.patch("singer.utils.parse_args")
class TestMainRoleAssumption(unittest.TestCase):
    def test_assumes_role_by_default(self, mock_parse_args, _mock_validate, mock_s3):
        config = {
            "start_date": "2021-11-02T00:00:00Z",
            "bucket": "b",
            "account_id": "123",
            "role_name": "r",
            "external_id": "e",
            "tables": "[]",
        }
        mock_parse_args.return_value = _base_args(config)

        tap_s3_csv.main()

        mock_s3.setup_aws_client.assert_called_once_with(config)
        mock_s3.setup_s3fs_client.assert_called_once_with(config)
        mock_s3.setup_aws_client_with_proxy.assert_not_called()

    def test_skips_role_when_opted_out(self, mock_parse_args, _mock_validate, mock_s3):
        config = {
            "start_date": "2021-11-02T00:00:00Z",
            "bucket": "b",
            "account_id": "123",
            "assume_role": False,
            "tables": "[]",
        }
        mock_parse_args.return_value = _base_args(config)

        tap_s3_csv.main()

        mock_s3.setup_aws_client.assert_not_called()
        mock_s3.setup_s3fs_client.assert_not_called()
        mock_s3.setup_aws_client_with_proxy.assert_not_called()

    def test_uses_proxy_when_configured(self, mock_parse_args, _mock_validate, mock_s3):
        config = {
            "start_date": "2021-11-02T00:00:00Z",
            "bucket": "b",
            "account_id": "123",
            "role_name": "r",
            "external_id": "e",
            "proxy_account_id": "456",
            "proxy_role_name": "pr",
            "tables": "[]",
        }
        mock_parse_args.return_value = _base_args(config)

        tap_s3_csv.main()

        mock_s3.setup_aws_client_with_proxy.assert_called_once_with(config)
        mock_s3.setup_s3fs_client_with_proxy.assert_called_once_with(config)
        mock_s3.setup_aws_client.assert_not_called()

    def test_raises_when_role_keys_missing(self, mock_parse_args, _mock_validate, mock_s3):
        config = {
            "start_date": "2021-11-02T00:00:00Z",
            "bucket": "b",
            "account_id": "123",
            "tables": "[]",
        }
        mock_parse_args.return_value = _base_args(config)

        with self.assertRaises(Exception):
            tap_s3_csv.main()

        mock_s3.setup_aws_client.assert_not_called()

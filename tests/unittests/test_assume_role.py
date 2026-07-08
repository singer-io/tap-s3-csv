import unittest
from unittest import mock

import tap_s3_csv


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
    def test_assumes_role_before_any_aws_call(self, mock_parse_args, _mock_validate, mock_s3):
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

        # The role is assumed via client setup and no bucket listing happens first.
        mock_s3.setup_aws_client.assert_called_once_with(config)
        mock_s3.setup_s3fs_client.assert_called_once_with(config)
        mock_s3.list_files_in_bucket.assert_not_called()
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
        mock_s3.list_files_in_bucket.assert_not_called()

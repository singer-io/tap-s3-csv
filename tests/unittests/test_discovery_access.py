import unittest
import sys
from unittest.mock import patch


for module_name in [
    "boto3",
    "s3fs",
    "botocore",
    "botocore.credentials",
    "botocore.exceptions",
    "botocore.session",
    "botocore.config",
    "botocore.paginate",
    "aiobotocore",
    "aiobotocore.credentials",
    "aiobotocore.session",
    "singer.schema_generation",
    "singer_encodings",
    "singer_encodings.avro",
    "singer_encodings.compression",
    "singer_encodings.csv",
    "singer_encodings.jsonl",
    "singer_encodings.parquet",
    "voluptuous",
]:
    if module_name not in sys.modules:
        from unittest.mock import MagicMock
        sys.modules[module_name] = MagicMock()

from tap_s3_csv.discover import discover_streams
from tap_s3_csv.exceptions import S3CsvForbiddenError


class DiscoveryAccessTests(unittest.TestCase):
    def _config(self):
        return {
            "bucket": "bucket",
            "tables": [
                {
                    "table_name": "accounts",
                    "search_pattern": "accounts.csv",
                    "search_prefix": "prefix/accounts",
                    "key_properties": ["id"],
                    "date_overrides": [],
                },
                {
                    "table_name": "contacts",
                    "search_pattern": "contacts.csv",
                    "search_prefix": "prefix/contacts",
                    "key_properties": ["id"],
                    "date_overrides": [],
                },
            ],
        }

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_all_accessible(self, mock_check_access, mock_discover_schema):
        mock_check_access.return_value = True
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": self._config()})()
        streams = discover_streams(self._config(), client=client)

        self.assertEqual({stream["tap_stream_id"] for stream in streams}, {"accounts", "contacts"})

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_excludes_inaccessible_streams(self, mock_check_access, mock_discover_schema):
        mock_check_access.side_effect = [True, False]
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": self._config()})()
        streams = discover_streams(self._config(), client=client)

        self.assertEqual({stream["tap_stream_id"] for stream in streams}, {"accounts"})

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_raises_if_no_stream_accessible(self, mock_check_access, mock_discover_schema):
        mock_check_access.return_value = False
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": self._config()})()
        with self.assertRaises(S3CsvForbiddenError):
            discover_streams(self._config(), client=client)

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_prunes_child_if_parent_removed(self, mock_check_access, mock_discover_schema):
        config = {
            "bucket": "bucket",
            "tables": [
                {
                    "table_name": "parent",
                    "search_pattern": "parent.csv",
                    "key_properties": ["id"],
                    "date_overrides": [],
                },
                {
                    "table_name": "child",
                    "search_pattern": "child.csv",
                    "key_properties": ["id"],
                    "date_overrides": [],
                    "parent": "parent",
                },
            ],
        }

        mock_check_access.side_effect = [False, True]
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": config})()
        with self.assertRaises(S3CsvForbiddenError):
            discover_streams(config, client=client)

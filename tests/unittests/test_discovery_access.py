import unittest
import sys
from importlib import import_module
from unittest.mock import MagicMock, patch


MODULE_NAMES = [
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
]

class DiscoveryAccessTests(unittest.TestCase):
    def setUp(self):
        self._module_patch = patch.dict(
            sys.modules,
            {module_name: MagicMock() for module_name in MODULE_NAMES},
        )
        self._module_patch.start()

    def tearDown(self):
        self._module_patch.stop()

    def _discover_streams(self):
        discover_module = import_module("tap_s3_csv.discover")
        return discover_module.discover_streams

    def _forbidden_error(self):
        exceptions_module = import_module("tap_s3_csv.exceptions")
        return exceptions_module.S3CsvForbiddenError

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
        discover_streams = self._discover_streams()
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
        discover_streams = self._discover_streams()
        mock_check_access.side_effect = [True, False]
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": self._config()})()
        streams = discover_streams(self._config(), client=client)

        self.assertEqual({stream["tap_stream_id"] for stream in streams}, {"accounts"})
        self.assertEqual(mock_discover_schema.call_count, 1)
        called_table_spec = mock_discover_schema.call_args[0][1]
        self.assertEqual(called_table_spec["table_name"], "accounts")

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_raises_if_no_stream_accessible(self, mock_check_access, mock_discover_schema):
        discover_streams = self._discover_streams()
        mock_check_access.return_value = False
        mock_discover_schema.return_value = {
            "type": "object",
            "properties": {"id": {"type": ["null", "string"]}},
        }

        client = type("Client", (), {"config": self._config()})()
        with self.assertRaises(self._forbidden_error()):
            discover_streams(self._config(), client=client)

    @patch("tap_s3_csv.discover.discover_schema")
    @patch("tap_s3_csv.discover.TableStream.check_access")
    def test_discovery_prunes_child_if_parent_removed(self, mock_check_access, mock_discover_schema):
        discover_streams = self._discover_streams()
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
        with self.assertRaises(self._forbidden_error()):
            discover_streams(config, client=client)

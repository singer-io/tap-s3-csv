import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie

import utils_for_test as utils
import unittest

CSV_FOLDER_PATH = "Compressed-CSV"
COMPRESSION_FOLDER_PATH = "Compressed"
JSONL_FOLDER_PATH = "Compressed-JSONL"

class S3CompressedFile(unittest.TestCase):

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return []

    def tap_name(self):
        return "tap-s3-csv"

    def get_type(self):
        return "platform.s3-csv"

    def name(self):
        return ""

    def expected_check_streams(self):
        return {
            ''
        }

    def expected_sync_streams(self):
        return {
            ''
        }

    def expected_pks(self):
        return {}

    def get_credentials(self):
        return {}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'bucket': 'com-stitchdata-prod-circleci-assets',
            'account_id': '218546966473',
        }

    
    def select_specific_catalog(self, found_catalogs, catalog_to_select):
        for catalog in found_catalogs:
            if catalog['tap_stream_id'] != catalog_to_select:
                continue

            schema = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
            non_selected_properties = []
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                self.conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )
            break


    def setUpTestEnvironment(self, folder_path):
        utils.delete_and_push_file(self.get_properties(), self.resource_names(), folder_path)

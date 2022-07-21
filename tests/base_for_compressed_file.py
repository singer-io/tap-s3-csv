from tap_tester import connections, menagerie, runner

import utils_for_test as utils

CSV_FOLDER_PATH = "Compressed-CSV"
COMPRESSION_FOLDER_PATH = "Compressed"
JSONL_FOLDER_PATH = "Compressed-JSONL"

class S3CompressedFile:

    def setUp(self):
        self.conn_id = connections.ensure_connection(self)

    def resource_names(self):
        return []

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

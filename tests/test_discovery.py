import unittest
from tap_tester import menagerie, runner, connections
from base import S3CSVBaseTest
from utils_for_test import delete_and_push_file

class S3StartDateTest(S3CSVBaseTest):
    """
        - Verify number of tables discovered matches expectations.
        - Verify table names follow naming convention (lowercase alphas and underscores).
        - Verify there is only 1 top level breadcrumb.
        - Verify there are no duplicate/conflicting metadata entries.
        - Verify primary key(s) match expectations.
        - Verify '_sdc' fields are added in the schema.
        - Verify the absence of a forced-replication-method (replication methods are non-discoverable for db taps).
        - Verify that primary keys and have inclusion of "automatic".
    """

    table_entry = [{'table_name': 'employees', 'key_properties': 'id', 'search_prefix': 'tap-s3-csv', 'search_pattern': 'discovery_test.csv', 'date_overrides': 'date of joining'}]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), "tap-s3-csv")
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["discovery_test.csv"]

    def name(self):
        return "test_start_date"

    def expected_check_streams(self):
        return {'employees'}

    def test_run(self):

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Skipping this assertion as this Tap is dynamic. So, it not necessary that name will be in the expected format all the time.
        # # Verify stream names follow naming convention
        # # streams should only have lowercase alphas and underscores
        # found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        # self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
        #                 msg="One or more streams don't follow standard naming")

        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # Collecting expected values
                expected_primary_keys = {'id'}

                # Collecting actual values
                schema_and_metadata = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(stream_properties[0].get("metadata", {"table-key-properties": []}).get("table-key-properties", []))
                actual_replication_method = set(stream_properties[0].get("metadata", {"forced-replication-method": []}).get("forced-replication-method", []))
                actual_automatic_fields = set(item.get("breadcrumb", ["properties", None])[1] for item in metadata
                                              if item.get("metadata").get("inclusion") == "automatic")

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                # Verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the metadata entries retrieved")

                # Verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # Verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # Verify '_sdc' fields are added in the schema.
                for _sdc_field in ["_sdc_source_bucket", "_sdc_source_file", "_sdc_source_lineno", "_sdc_extra"]:
                    self.assertTrue(_sdc_field in actual_fields)

                # Verify the absence of a 'forced-replication-method' (replication methods are non-discoverable for db taps).
                self.assertEqual(actual_replication_method, set())

                # Verify that primary keys are given the inclusion of automatic in metadata
                self.assertSetEqual(expected_primary_keys, actual_automatic_fields)

                # Verify that all other fields have inclusion of available
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")

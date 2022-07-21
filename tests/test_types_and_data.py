"""
A module for testing the s3 csv tap
"""
import csv
import json
import unittest
import os
from enum import Enum
from functools import reduce

import boto3
from singer import metadata

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from base import S3CSVBaseTest

TARGET_OUTPUT_FILE = '/tmp/stitch-target-out.json'

EXPECTED_STREAMS_TO_RESOURCES = {
    "test_data_types_no_coercion": {"tap-s3-csv/test_data_types.csv"},
    "pk_unique_values_nullable_and_non_nullable":
        {"tap-s3-csv/primary_key_unique_values_and_nullable_integers.csv"},
    "multiple_files_one_stream": {
        "tap-s3-csv/test_multiple_file_with_pk_part_c.csv",
        "tap-s3-csv/test_multiple_file_with_pk_part_b.csv",
        "tap-s3-csv/test_multiple_file_with_pk_part_a.csv",
    },
    "multiple_files_one_stream_unordered_header": {
        "tap-s3-csv/test_header_order_multiple_file_with_pk_part_b.csv",
        "tap-s3-csv/test_header_order_multiple_file_with_pk_part_a.csv"
    },
    "not real numbers": {"tap-s3-csv/test_not_real_numbers.csv"},
    "date time min and max values": {"tap-s3-csv/test_date_time_iso_format_min_max_values.csv"},
    "primary key 56 null": {"tap-s3-csv/test_primary_key_56_null.csv"},
    "primary key 56 duplicate": {"tap-s3-csv/test_primary_key_56_duplicate.csv"},
    "all utf-8 unicode characters": {"tap-s3-csv/test_unicode_characters.csv"},
    "problematic headers": {"tap-s3-csv/problematic_header_characters.csv"},
    "multiple_files_header_different": {
        "tap-s3-csv/multiple_files_header_different_part_a.csv",
        "tap-s3-csv/multiple_files_header_different_part_b.csv"},
    "test_switching_data_types": {"tap-s3-csv/test_switching_data_types.csv"},
    "header_longer": {"tap-s3-csv/header_longer.csv"},
    "header_shorter": {"tap-s3-csv/header_shorter.csv"},
    "rows_longer_and_shorter": {"tap-s3-csv/rows_longer_and_shorter.csv"}
}

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)

class DataTypes(Enum):
    """
    An Enumeration capturing all of the expected schema types allowed for tap-s3-csv
    """

    NULL_INTEGER = ("type", ["null", "integer", "string"])
    NULL_NUMBER = ("type", ["null", "number", "string"])
    NULL_STRING = ("type", ["null", "string"])
    NULL_DATE_TIME = \
        ("anyOf", [{
            "format": "date-time",
            "type": ["null", "string"]
        }, {
            "type": ["null", "string"]
        }])
    INTEGER = ("type", "integer")
    STRING = ("type", "string")
    ARRAY = ("type", "array")


class S3TypesAndData(S3CSVBaseTest):
    """
    A set of test cases to verify tap-s3-csv

    This will verify supported data types are being sampled
    and schema is being created correctly. That meta-data is
    well formed and that the primary keys are identified and
    automatically selected by the tap.  Additional fields
    will be selectable after discovery.

    Testing will focus on positive test cases with some error
    handling testing.
    """

    conn_id = None

    @classmethod
    def setUpClass(cls):

        s3_client = boto3.resource('s3')
        properties = cls.get_properties()

        # Parsing the properties tables is a hack for now.
        tables = json.loads(properties['tables'])

        s3_bucket = s3_client.Bucket(properties['bucket'])

        # Get into a clean state by removing all objects from the bucket
        # s3_bucket.objects.all().delete()

        for resource_name in cls.resource_names():
            # this assumes the search_prefix is always the same. Not sure that can be guaranteed.
            s3_path = tables[0]['search_prefix'] + '/' + resource_name
            s3_object = s3_bucket.Object(s3_path)

            # Put S3 File to com-stitchdata-prod-circleci-assets/s3_discovery_test
            print("Attempting to upload S3 file {} before test.".format(resource_name))
            s3_object.upload_file(get_resources_path(resource_name))
        cls.conn_id = connections.ensure_connection(cls)

    @staticmethod
    def resource_names():
        """Returns a list of files required to be uploaded to S3 for test setup"""
        return set.union(*EXPECTED_STREAMS_TO_RESOURCES.values())

    @staticmethod
    def columns_in_header_of_csv_file(resource_name) -> set:
        """
        Reads the first line of the csv file provided and returns the headers

        Args:
            resource_name: The file name to read which must be in the
                resources folder.

        Returns:
            A set of column names from the header row of the csv file specified.
        """
        return_value = set()
        for resource in resource_name:
            with open(get_resources_path(resource), 'r', encoding='utf-8') as file:
                read = csv.reader(file)

                # get the first fow of the csv file and return it as a set
                headers = set(next(read))
                return_value = return_value.union(headers)
        return return_value

    @staticmethod
    def expected_stream_row_counts() -> dict:
        return_value = {}
        streams = S3TypesAndData.expected_stream_names()
        for stream in streams:
            files = list(EXPECTED_STREAMS_TO_RESOURCES[stream])
            for file in files:
                with open(get_resources_path(file), 'r', encoding='utf-8') as f:
                    for i, l in enumerate(f):
                        pass

                    # zero based but we don't want to count the header
                    return_value[stream] = return_value.get(stream, 0) + i
                    if stream == "all utf-8 unicode characters":
                        # There are 2 null characters that couldn't be decoded
                        return_value[stream] -= 2

        return return_value

    @staticmethod
    def stitch_added_columns():
        """
        A set of expected properties added by the tap as metadata

        Returns:
            A set of of column names
        """
        return {"_sdc_source_lineno", "_sdc_source_bucket", "_sdc_extra", "_sdc_source_file"}

    @staticmethod
    def name():
        """
        specifies the name of the test to use with the runner
        """

        return "tap_tester_sdc_csv_types_and_data"

    @staticmethod
    def configuration():
        """
        Returns:
            the name of the configuration file for the tap
        """
        return "tap-s3-csv/configuration.json"

    @staticmethod
    def get_properties() -> dict:
        """
        Reads the configuration file to get the table information, bucket
        and replication start date.

        The table information is double encoded as a string and must be converted
        back to json prior to returning.

        This is used by the connection service.

        Returns:
            A dictionary of table information and other properties from the configuration file
        """

        # TODO - change bucket to "com-stitchdata.vm.tap-tester.tap-s3-csv"

        with open(get_resources_path(S3TypesAndData.configuration()), encoding='utf-8') as file:
            data = json.load(file)
            data["tables"] = json.dumps(data["tables"])

        return data

    @staticmethod
    def expected_streams() -> list:
        """
        The list of expected streams based on the configuration file

        Returns:
            A list of stream names from the configuration file
        """
        streams_data = S3TypesAndData.get_properties()["tables"]
        return json.loads(streams_data)

    @staticmethod
    def expected_check_streams() -> set:
        return {stream["table_name"] for stream in S3TypesAndData.expected_streams()}

    @staticmethod
    def expected_stream_names() -> set:
        """
        gets the name for each expected stream

        Returns:
            A set of names for all expected streams
        """
        return {x["table_name"] for x in S3TypesAndData.expected_streams()}

    @staticmethod
    def expected_pks() -> dict:
        """
        parses a list of expected streams for and gets a list of key properties for each

        trims whitespace and removes length = 0 keys from the key properties

        Returns:
            A dictionary of keys per stream for all expected streams
            where the key is the table name and the value is the key_properties
        """
        return_value = {}
        for stream in S3TypesAndData.expected_streams():
            value = stream.get("key_properties", "").split(",")
            value = {v.strip() for v in value if v.strip() != ""}
            return_value[stream["table_name"]] = value

        return return_value

    @staticmethod
    def expected_properties_for_data_types(filter_type: DataTypes, table):
        """
        Looks at the expected schema for a data type and table and determines the
        properties with that type in that table.

        Args:
            The data type to filter properties based on
            The stream name of the schema

        Returns:
            A set of properties that should have the specified data type
        """
        return {key for key, value
                in S3TypesAndData.expected_schema_for_data_types(table).items()
                if value == filter_type.value}

    @staticmethod
    def expected_schema_for_data_types(table):
        """
        properties with the expected type required for the data type test
            test_data_type_sampling

        Returns:
            A dictionary of properties with associated type data
        """

        schemas = \
            {
                "test_switching_data_types":
                    {
                        "integer_to_number": DataTypes.NULL_NUMBER.value,
                        "number_to_integer": DataTypes.NULL_NUMBER.value,
                        "number_to_string": DataTypes.NULL_STRING.value,
                        "string_to_integer": DataTypes.NULL_STRING.value,
                        "string_to_date_time": DataTypes.NULL_STRING.value,
                        "date_time_to_integer": DataTypes.NULL_STRING.value,
                        "date_time_to_integer_override": DataTypes.NULL_DATE_TIME.value,
                        "_sdc_source_lineno": DataTypes.INTEGER.value,
                        "_sdc_source_file": DataTypes.STRING.value,
                        "_sdc_extra": DataTypes.ARRAY.value,
                        "_sdc_source_bucket": DataTypes.STRING.value
                    },
                "test_data_types_no_coercion":
                    {
                        "positive with various signs": DataTypes.NULL_NUMBER.value,
                        "float with precision": DataTypes.NULL_NUMBER.value,

                        # FIXME - should this be a number
                        "positive percentages": DataTypes.NULL_STRING.value,
                        "time to microseconds": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz to milliseconds": DataTypes.NULL_DATE_TIME.value,
                        "string length test": DataTypes.NULL_STRING.value,
                        "date": DataTypes.NULL_DATE_TIME.value,
                        "time to minutes": DataTypes.NULL_DATE_TIME.value,
                        "datetime without tz to seconds": DataTypes.NULL_DATE_TIME.value,
                        "time": DataTypes.NULL_DATE_TIME.value,
                        "length of string": DataTypes.NULL_INTEGER.value,
                        "datetime with tz space separator": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz to microseconds": DataTypes.NULL_DATE_TIME.value,
                        "larger_than_bigint": DataTypes.NULL_INTEGER.value,
                        "time to seconds": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz to minutes": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz to hour": DataTypes.NULL_DATE_TIME.value,
                        "float at the edges": DataTypes.NULL_NUMBER.value,
                        "bigint_signed": DataTypes.NULL_INTEGER.value,
                        "negative with signs": DataTypes.NULL_NUMBER.value,
                        "double at the edges": DataTypes.NULL_NUMBER.value,

                        # FIXME - should this be a number
                        "positive numbers with commas": DataTypes.NULL_STRING.value,
                        "time to hour": DataTypes.NULL_INTEGER.value,
                        "datetime without tz": DataTypes.NULL_DATE_TIME.value,
                        "datetime with tz to seconds": DataTypes.NULL_DATE_TIME.value,
                        "datetime without tz to minutes": DataTypes.NULL_DATE_TIME.value,
                        "bigint_unsigned": DataTypes.NULL_INTEGER.value,

                        # FIXME - should this be a number
                        "negative numbers with commas": DataTypes.NULL_STRING.value,
                        "datetime without tz to milliseconds": DataTypes.NULL_DATE_TIME.value,
                        "datetime without tz space separator": DataTypes.NULL_DATE_TIME.value,
                        "time to milliseconds": DataTypes.NULL_DATE_TIME.value,
                        "datetime without tz to microseconds": DataTypes.NULL_DATE_TIME.value,

                        # FIXME - should this be a number
                        "negative percentages": DataTypes.NULL_STRING.value,
                        "datetime without tz to hour": DataTypes.NULL_DATE_TIME.value,
                        "_sdc_source_lineno": DataTypes.INTEGER.value,
                        "_sdc_source_file": DataTypes.STRING.value,
                        "_sdc_extra": DataTypes.ARRAY.value,
                        "_sdc_source_bucket": DataTypes.STRING.value
                    }
            }

        return schemas[table]

    def test_000_run(self):
        """
        run discovery as the first test and ensure that it completed as expected.
        """
        runner.run_check_job_and_check_status(self)

    def test_discovery(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.
        """
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # verify that the number of streams is correct based on the configuration
        self.assertEqual(len(found_catalogs), len(self.expected_streams()),
                         "The number of catalogs doesn't match "
                         "the number of tables in the configuration")

        # verify the stream names are the names in the config file -- with " " -> "_"?
        found_stream_names = {x["stream_name"] for x in found_catalogs}
        self.assertEqual(found_stream_names, self.expected_stream_names())

        # verify the number of top level objects in the schema is correct
        for catalog in found_catalogs:
            with self.subTest(c=catalog):
                stream_name = catalog["stream_name"]
                files_for_stream = list(EXPECTED_STREAMS_TO_RESOURCES[stream_name])
                expected_properties = S3TypesAndData.columns_in_header_of_csv_file(
                    files_for_stream).union(S3TypesAndData.stitch_added_columns())

                metadata_and_annotated_schema = menagerie.get_annotated_schema(
                    S3TypesAndData.conn_id, catalog['stream_id'])
                annotated_schema = metadata_and_annotated_schema["annotated-schema"]
                metadata = metadata_and_annotated_schema["metadata"]
                
                # verify that the annotated schema has the correct number of properties
                self.assertEqual(
                    len(expected_properties),
                    len(annotated_schema.get("properties").keys()))

                # verify that the metadata has the correct number of breadcrumbs with properties
                properties_metadata = [x for x in metadata if "properties" in x.get("breadcrumb")]
                self.assertEqual(len(expected_properties), len(properties_metadata))

                # verify that all non pk's are given the inclusion of available in annotated schema.
                expected_key_properties = \
                    S3TypesAndData.expected_pks()[stream_name]
                expected_not_pk_properties = expected_properties.difference(expected_key_properties)
                actual_available_properties = {k for k, v in annotated_schema["properties"].items()
                                               if v.get("inclusion") == "available"}
                self.assertEqual(actual_available_properties, expected_not_pk_properties)

                # verify that all non pk's are given the inclusion of available in metadata.
                # make sure that we use problematic characters for header names
                #   - space" ", dash"-", underscore"_", comma"," etc.
                actual_available_properties = \
                    {item.get("breadcrumb", ["", ""])[1]
                     for item in metadata
                     if item.get("metadata").get("inclusion") == "available"}
                self.assertEqual(actual_available_properties, expected_not_pk_properties)

    def test_data_type_sampling(self):
        """
        Verify that each data type can be sampled and determined correctly.

        A file for stream `test_data_types_no_coercion` was setup which
        has one column for each test. Tests include each data type:
            * integer
            * number
            * date-time
            * string

        integers are tested for boundary conditions of signed and unsigned big-ints,
        strings ore tested for length including a null string and 65536 chars.
        numbers are tested for float and double representations at the borders
            which are exponents for extremely large and small positive and
            negative numbers plus zero.  numbers are also tested for precision
        date-times are tested at the borders of allowed python date-times in
            multiple types of formats including just dates, just times, date-times
            with timezone and date-times without timezone.

        The test below uses subtests so that each data-type is tested and
        reported on individually
        """

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # only testing the data types stream for now, may want to test all of them
        # or add more tests for different things for other catalogs.
        data_type_catalogs = [x for x in found_catalogs
                             if x["stream_name"] in ("test_data_types_no_coercion",
                                                     "test_switching_data_types")]

        for data_type in DataTypes:
            for catalog in data_type_catalogs:
                with self.subTest(dt=(data_type, catalog)):

                    # verify each data type is sampled correctly in the annotated-schema
                    expected_properties = S3TypesAndData.expected_properties_for_data_types(
                        data_type, catalog['stream_name'])

                    metadata_and_annotated_schema = menagerie.get_annotated_schema(
                        S3TypesAndData.conn_id, catalog['stream_id'])
                    properties = metadata_and_annotated_schema["annotated-schema"]["properties"]
                    actual_properties = {k for k, v in properties.items()
                                         if v.get(data_type.value[0]) == data_type.value[1]}
                    self.assertEqual(expected_properties, actual_properties)

    def test_primary_keys(self):
        """
        Verify that the configuration can be used to set primary key fields when
            * the primary key is an empty list
            * the primary key is a single field
            * the primary key is a composite of multiple fields
        """
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)
        all_catalogs = [x for x in found_catalogs]
        for catalog in all_catalogs:
            with self.subTest(c=catalog):
                expected_key_properties = \
                    S3TypesAndData.expected_pks()[catalog["stream_name"]]
                metadata_and_annotated_schema = menagerie.get_annotated_schema(
                    S3TypesAndData.conn_id, catalog['stream_id'])

                # verify that expected_key_properties show as automatic in metadata
                metadata = metadata_and_annotated_schema["metadata"]
                actual_key_properties = {item.get("breadcrumb", ["", ""])[1]
                                         for item in metadata
                                         if item.get("metadata").get("inclusion") == "automatic"}
                self.assertEqual(actual_key_properties, expected_key_properties)

        # TODO - what happens if a csv file is updated to have an extra column?
        #  self.assertEqual(annotated_schema['annotated-schema'], expected_schemas)

        # TODO  - Test header and data rows with different lengths
        #       - row data longer than header > data in array in _s3_extra
        #       - header longer than row data > nulls at end of row data
        #       - test a combo of above

        # TODO - Test configuration of date_overrides that don't exist

    def test_zzzu_run_sync_mode(self):
        # Select our catalogs
        our_catalogs = self.run_and_verify_check_mode(self.conn_id)
        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs, True)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(self.conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(
            self,
            self.conn_id,
            self.expected_check_streams(),
            self.expected_pks())
        replicated_row_count = reduce(lambda accum, c : accum + c, record_count_by_stream.values())

        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):
                self.assertEqual(
                    record_count_by_stream.get(stream, 0),
                    S3TypesAndData.expected_stream_row_counts()[stream],
                    msg="actual rows: {}, expected_rows: {} for stream {} don't match".format(
                        record_count_by_stream.get(stream, 0),
                        S3TypesAndData.expected_stream_row_counts()[stream],
                        stream)
                )

        print("total replicated row count: {}".format(replicated_row_count))

        synced_records = runner.get_records_from_target_output()

        # verify that when header is longer, the end columns have null values
        upsert_message_header_longer = [m for m in synced_records.get('header_longer').get('messages') if m['action'] == 'upsert']
        data_null = [d for d in upsert_message_header_longer
                if d["data"].get("aa0") == d["data"].get("ab0") == d["data"].get("ac0")
                == d["data"].get("ad0") == d["data"].get("ae0") is None]
        self.assertEqual(94, len(data_null))

        # verify that when header is shorter, the _sdc_extra has the values
        upsert_message_header_shorter = [m for m in synced_records.get('header_shorter').get('messages') if m['action'] == 'upsert']
        s3_extra = [d for d in upsert_message_header_shorter
                    if len(d["data"]["_sdc_extra"]) == 1]
        self.assertEqual(
            S3TypesAndData.expected_stream_row_counts()['header_shorter'],
            len(s3_extra))

        # verify when one row is shorter and one longer one has _sdc_extra other has null
        upsert_message_rows_longer_shorter = [m for m in synced_records.get('rows_longer_and_shorter').get('messages') if m['action'] == 'upsert']
        data_null = [d for d in upsert_message_rows_longer_shorter
                        if d["data"].get("v0") == d["data"].get("w0") == d["data"].get("x0")
                        == d["data"].get("y0") == d["data"].get("z0") is None]
        s3_extra = [d for d in upsert_message_rows_longer_shorter
                    if len(d["data"].get("_sdc_extra", [])) == 1]
        self.assertTrue(len(data_null) == len(s3_extra) == 1)

import json
import boto3
import unittest
import os

import tap_tester.connections as connections
import tap_tester.runner as runner
from base import S3CSVBaseTest

def get_resources_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', path)

class S3NoFilesMatch(S3CSVBaseTest):

    @staticmethod
    def name():
        """
        specifies the name of the test to use with the runner
        """

        return "tap_tester_s3_csv_no_files_match"

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

        with open(get_resources_path("tap-s3-csv/no_files_match_config.json"), encoding='utf-8') as file:
            data = json.load(file)
            data["tables"] = json.dumps(data["tables"])

        return data

    @classmethod
    def setUpClass(cls):
        s3_client = boto3.resource('s3')
        properties = cls.get_properties()

        # Parsing the properties tables is a hack for now.
        tables = json.loads(properties['tables'])

        s3_bucket = s3_client.Bucket(properties['bucket'])

        resource_name = "tap-s3-csv/primary_key_unique_values_and_nullable_integers.csv"
        s3_path = tables[0]['search_prefix'] + '/' + resource_name
        s3_object = s3_bucket.Object(s3_path)

        # Put S3 File to com-stitchdata-prod-circleci-assets/s3_discovery_test
        print("Attempting to upload S3 file {} before test.".format(resource_name))
        s3_object.upload_file(get_resources_path(resource_name))
        cls.conn_id = connections.ensure_connection(cls)


    def test_missing_key_properties(self):
        self.assertRaisesRegex(Exception, ".*No files found.*", runner.run_check_job_and_check_status, self)

import unittest
from unittest import mock
from tap_s3_csv import s3

def mock_empty_sample_files(config, table_spec, s3_files_gen):
    samples = []
    for sample in samples:
        yield sample

def mock_valid_sample_files(config, table_spec, s3_files_gen):
    samples = [{'id': 1, 'name': 'Bob'}, {'id': 2, 'name': 'Alice'}]
    for sample in samples:
        yield sample

@mock.patch("tap_s3_csv.s3.get_input_files_for_table")
class TestGetSampledSchema(unittest.TestCase):

    @mock.patch("tap_s3_csv.s3.sample_files", side_effect=mock_empty_sample_files)
    def test_schema_for_no_samples(self, mock_sample_files, mocked_get_input_file):
        '''
            properties should be empty if no samples found from any files
        '''
        config = {}
        table_spec = {}
        returned_schema = s3.get_sampled_schema_for_table(config, table_spec)
        expected_schema = {
            'type': 'object',
            'properties': {}
        }
        self.assertEqual(returned_schema, expected_schema)

    @mock.patch("tap_s3_csv.s3.sample_files", side_effect=mock_valid_sample_files)
    def test_schema_for_valid_samples(self, mock_valid_sample_files, mocked_get_input_file):
        '''
            properties should be as per samples if samples found from any files used in sampling
        '''
        config = {}
        table_spec = {}
        returned_schema = s3.get_sampled_schema_for_table(config, table_spec)
        expected_schema =  {
            'type': 'object',
            'properties': {
            'id': {
                'type': [
                'null',
                'integer',
                'string'
                ]
            },
            'name': {
                'type': [
                'null',
                'string'
                ]
            },
            '_sdc_source_bucket': {
                'type': 'string'
            },
            '_sdc_source_file': {
                'type': 'string'
            },
            '_sdc_source_lineno': {
                'type': 'integer'
            },
            '_sdc_extra': {
                'type': 'array',
                'items': {
                'anyOf': [
                    {
                    'type': 'object',
                    'properties': {}
                    },
                    {
                    'type': 'string'
                    }
                ]
                }
            }
            }
        }

        print(returned_schema)
        self.assertEqual(returned_schema, expected_schema)

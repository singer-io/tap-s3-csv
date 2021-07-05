import unittest
from unittest import mock
from tap_s3_csv import sync

def mockTransformer():
    class Transformer():
        def __init__(self):
            pass
        def transform(self,rec,schema,matadata):
            return rec
    return Transformer()

@mock.patch("tap_s3_csv.sync.csv_helper.get_row_iterator")
@mock.patch("singer.Transformer",side_effect=mockTransformer)
class TestKeyErrorFOrCatalogPropeties(unittest.TestCase):

    def test_catalog_with_properties(self, mockTransformer, mocked_get_row_iterator):
        config = {'bucket': 'test'}
        table_spec = {'table_name': 'test_table'}
        stream = {"schema": {"properties": {"columnA": {"type": ["integer"]}}}}
        file_handle = [
            b"columnA,columnB,columnC",
            b"1,2,3,4"
        ]
        s3_path = "unittest/sample.csv"

        sync.sync_csv_file(config, file_handle, s3_path, table_spec, stream)
        mocked_get_row_iterator.assert_called_with(file_handle, table_spec, stream["schema"]["properties"].keys(), True)

    def test_catalog_with_no_properties(self, mockTransformer, mocked_get_row_iterator):
        config = {'bucket': 'test'}
        table_spec = {'table_name': 'test_table'}
        stream = {"schema": {}}
        file_handle = [
            b"columnA,columnB,columnC",
            b"1,2,3,4"
        ]
        s3_path = "unittest/sample.csv"
        
        sync.sync_csv_file(config, file_handle, s3_path, table_spec, stream)
        mocked_get_row_iterator.assert_called_with(file_handle, table_spec, None, True)

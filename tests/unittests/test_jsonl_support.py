import unittest
from unittest import mock
from tap_s3_csv import s3
from tap_s3_csv import sync

def mockclass():
    class Transformer():
        def __init__(self):
            pass
        def transform(self,rec,schema,matadata):
            return rec
    return Transformer()

def mock_get_records_for_jsonl(s3_path, sample_rate, iterator):
    mock_records = [{"name": "test", "id": 5}]
    return mock_records

def mock_get_records_for_csv(s3_path, sample_rate, iterator):
    mock_records = [{"name": "test", "id": 5}]
    return mock_records

def mock_json_file_handler_5_records_for_s3(config, s3_path):
    class file_handle():
        def __init__(self, _raw_stream):
            self._raw_stream = _raw_stream
    _raw_stream = [
        b'{"name":"test","id":"1"}\n',
        b'{"name":"test1","id":"2"}\n',
        b'{"name":"test2","id":"3"}\n',
        b'{"name":"test3","id":"4"}\n',
        b'{"name":"test4","id":"5","marks":"[\'221\',\'222\']"}\n'
    ]
    return file_handle(_raw_stream)


def mock_sync_jsonl_file(config, iterator, s3_path, table_spec, stream):
    return 1

def mock_sync_csv_file(config, file_handle, s3_path, table_spec, stream):
    return 1

class TestJsonlSupport(unittest.TestCase):
    '''
    Unit tests of funtions:

    s3.py
    get_records_for_jsonl
    sample_file - Check get_records_for_jsonl is calling for jsonl

    sync.py
    sync_jsonl_file
    sync_table_file - Check sync_jsonl_file is calling for jsonl
    '''
    

    @mock.patch("tap_s3_csv.s3.check_key_properties_and_date_overrides_for_jsonl_file")
    def test_get_records_for_jsonl_in_sample_file_for_5_records_of_file_with_sample_rate_2(self, mocked_check_key_properties_and_date_overrides_for_jsonl_file):

        s3_path = "test/abc.jsonl"
        file_handle = [
            b'{"name":"test","id":"1"}\n',
            b'{"name":"test1","id":"2"}\n',
            b'{"name":"test2","id":"3"}\n',
            b'{"name":"test3","id":"4"}\n',
            b'{"name":"test4","id":"5","marks":"[\'221\',\'222\']"}\n'
        ]
        sample_rate = 2
        config = None
        table_spec = None
        expected_result = [
            {"name":"test","id":"1"},
            {"name":"test2","id":"3"},
            {"name":"test4","id":"5","marks":"['221','222']"}
        ]
        result = s3.sample_file(table_spec, s3_path, file_handle, sample_rate, "jsonl")
        self.assertListEqual(list(result),expected_result)


    @mock.patch("tap_s3_csv.s3.check_key_properties_and_date_overrides_for_jsonl_file")
    def test_get_records_for_jsonl_in_sample_file_for_10_records_of_file_with_sample_rate_3(self,mock_check_key_properties_and_date_overrides_for_jsonl_file):

        s3_path = "test/abc.jsonl"
        file_handle = [
            b'{"name":"test","id":"1"}\n',
            b'{"name":"test1","id":"2"}\n',
            b'{"name":"test2","id":"3"}\n',
            b'{"name":"test3","id":"4"}\n',
            b'{"name":"test4","id":"5","marks":"[\'221\',\'222\']"}\n',
            b'{"name":"test5","id":"6"}\n',
            b'{"name":"test6","id":"7"}\n',
            b'{"name":"test7","id":"8"}\n',
            b'{"name":"test8","id":"9"}\n',
            b'{"name":"test9","id":"10","marks":"[\'111\',\'112\']"}\n'
        ]
        sample_rate = 3
        config = None
        table_spec = None
        expected_result = [
            {"name":"test","id":"1"},
            {"name":"test3","id":"4"},
            {"name":"test6","id":"7"},
            {"name":"test9","id":"10","marks":"['111','112']"}
        ]
        result = s3.sample_file(table_spec, s3_path, file_handle, sample_rate, "jsonl")
        self.assertListEqual(list(result),expected_result)


    @mock.patch('singer.Transformer',side_effect=mockclass)
    def test_sync_jsonl_file_with_total_2_record(self,mockclass):

        s3_path = "test/abc.jsonl"
        file_handle = [
            b'{"name":"test","id":"3","tt":"tetete"}\n',
            b'{"name":"test1","id":"4","tt":"tetete","ab":"bb","mark11":["221","222"],"student11":{"no":5,"col":6}}\n',
        ]
        table_spec = {'table_name': 'test_table'}

        config = {'bucket':'Test'}
        stream = {'stream': 'jsonl_table', 'tap_stream_id': 'jsonl_table', 'schema': {'type': 'object', 'properties': {'name': {'type': ['string', 'null']}, 'id': {'type': ['integer', 'string', 'null']}, 'marks': {'type': 'array', 'items': {'type': ['integer', 'string', 'null']}}, 'students': {'type': 'object', 'properties': {}}, '_sdc_source_bucket': {'type': 'string'}, '_sdc_source_file': {'type': 'string'}, '_sdc_source_lineno': {'type': 'integer'}, '_sdc_extra': {'type': 'array', 'items': {'type': 'string'}}}}, 'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'table-key-properties': ['id']}}, {'breadcrumb': ['properties', 'name'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ['properties', 'marks'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'students'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_bucket'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_file'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_lineno'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_extra'], 'metadata': {'inclusion': 'available'}}]}

        result = sync.sync_jsonl_file(config, file_handle, s3_path, table_spec, stream)
        self.assertEqual(result,2)


    @mock.patch('singer.Transformer',side_effect=mockclass)
    def test_sync_jsonl_file_with_total_5_record(self,mockclass):

        s3_path = "test/abc.jsonl"
        file_handle = [
            b'{"name":"test","id":"0","tt":"tetete"}\n',
            b'{"name":"test1","id":"1","tt":"tetete"}\n',
            b'{"name":"test2","id":"2","tt":"tetete"}\n',
            b'{"name":"test3","id":"3","tt":"tetete"}\n',
            b'{"name":"test4","id":"4","tt":"tetete","ab":"bb","mark11":["221","222"],"student11":{"no":5,"col":6}}\n',
        ]
        table_spec = {'table_name': 'test_table'}

        config = {'bucket':'Test'}
        stream = {'stream': 'jsonl_table', 'tap_stream_id': 'jsonl_table', 'schema': {'type': 'object', 'properties': {'name': {'type': ['string', 'null']}, 'id': {'type': ['integer', 'string', 'null']}, 'marks': {'type': 'array', 'items': {'type': ['integer', 'string', 'null']}}, 'students': {'type': 'object', 'properties': {}}, '_sdc_source_bucket': {'type': 'string'}, '_sdc_source_file': {'type': 'string'}, '_sdc_source_lineno': {'type': 'integer'}, '_sdc_extra': {'type': 'array', 'items': {'type': 'string'}}}}, 'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'table-key-properties': ['id']}}, {'breadcrumb': ['properties', 'name'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ['properties', 'marks'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'students'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_bucket'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_file'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_lineno'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_extra'], 'metadata': {'inclusion': 'available'}}]}

        result = sync.sync_jsonl_file(config, file_handle, s3_path, table_spec, stream)
        self.assertEqual(result,5)

    def test_check_key_properties_and_date_overrides_for_jsonl_file_key_properties_not_available(self):
        try:
            table_spec = {'search_prefix': '', 'search_pattern': 'test\\/.*\\.jsonl', 'table_name': 'jsonl_table', 'key_properties': ['idea'],'date_overrides': ['created_at'], 'delimiter': ','}
            jsonl_sample_records = [{"name":"test4","id":"4","tt":"tetete","ab":"bb","mark11":["221","222"],"student11":{"no":5,"col":6}}]
            s3_path = "test.jsonl"
            s3.check_key_properties_and_date_overrides_for_jsonl_file(table_spec,jsonl_sample_records, s3_path)
        except Exception as err:
            expected_message = "JSONL file \"test.jsonl\" is missing required key_properties key: {'idea'}"
            self.assertEqual(str(err), expected_message)

    def test_check_key_properties_and_date_overrides_for_jsonl_file_date_overrides_not_available(self):
        try:
            table_spec = {'search_prefix': '', 'search_pattern': 'test\\/.*\\.jsonl', 'table_name': 'jsonl_table', 'key_properties': ['id'],'date_overrides': ['created_at'], 'delimiter': ','}
            jsonl_sample_records = [{"name":"test4","id":"4","tt":"tetete","ab":"bb","mark11":["221","222"],"student11":{"no":5,"col":6}}]
            s3_path = "test.jsonl"
            s3.check_key_properties_and_date_overrides_for_jsonl_file(table_spec,jsonl_sample_records, s3_path)
        except Exception as err:
            expected_message = "JSONL file \"test.jsonl\" is missing date_overrides key: {'created_at'}"
            self.assertEqual(str(err), expected_message)

    def test_check_key_properties_and_date_overrides_for_jsonl_file_no_exception(self):
        try:
            table_spec = {'search_prefix': '', 'search_pattern': 'test\\/.*\\.jsonl', 'table_name': 'jsonl_table', 'key_properties': ['id'],'date_overrides': ['created_at'], 'delimiter': ','}
            jsonl_sample_records = [{"name":"test4","id":"4","tt":"tetete","ab":"bb","mark11":["221","222"],"created_at":"24-12-1996","student11":{"no":5,"col":6}}]
            s3_path = "test.jsonl"
            s3.check_key_properties_and_date_overrides_for_jsonl_file(table_spec,jsonl_sample_records, s3_path)
        except Exception:
            pass
    
    @mock.patch("tap_s3_csv.s3.get_records_for_jsonl", side_effect=mock_get_records_for_jsonl)
    @mock.patch("tap_s3_csv.s3.check_key_properties_and_date_overrides_for_jsonl_file")
    def test_get_record_for_jsonl_called_in_sample_file_for_jsonl_file(self, mock_get_records_for_jsonl, mock_key_properties):

        s3_path = "test\\abc.jsonl"
        table_spec = {'table_name': 'test_table'}
        file_handle = [
            b'{"name":"test","id":"1"}\n',
            b'{"name":"test1","id":"2"}\n',
            b'{"name":"test2","id":"3"}\n',
            b'{"name":"test3","id":"4"}\n',
            b'{"name":"test4","id":"5","marks":"[\'221\',\'222\']"}\n'
        ]
        sample_rate = 5

        s3.sample_file(table_spec, s3_path, file_handle, sample_rate, "jsonl")
        self.assertEqual(mock_get_records_for_jsonl.call_count, 1)


    def test_get_record_for_csv_called_in_sample_file_for_csv_file(self):

        s3_path = "test\\abc.csv"
        table_spec = {'table_name': 'test_table'}
        file_handle = [
            b'a,b,c',
            b'a0,b0,c0',
        ]
        sample_rate = 5

        records = s3.sample_file(table_spec, s3_path, file_handle, sample_rate, "csv")
        self.assertEqual(len(list(records)), 1)

    def test_get_record_for_csv_called_in_sample_file_for_txt_file(self):

        s3_path = "test\\abc.txt"
        table_spec = {'table_name': 'test_table'}
        file_handle = [
            b'a,b,c',
            b'a0,b0,c0',
        ]
        sample_rate = 5

        records = s3.sample_file(table_spec, s3_path, file_handle, sample_rate, "txt")
        self.assertEqual(len(list(records)), 1)

    @mock.patch("tap_s3_csv.s3.get_file_handle", side_effect=mock_json_file_handler_5_records_for_s3)
    @mock.patch("tap_s3_csv.sync.sync_jsonl_file", side_effect=mock_sync_jsonl_file)
    def test_sync_jsonl_file_called_in_sync_table_file_for_jsonl_file(self, mock_sync_jsonl_file, mocked_file_handle):

        s3_path = "test\\abc.jsonl"
        table_spec = {'table_name': 'test_table'}
        config = {'bucket': 'Test'}
        stream = {'stream': 'jsonl_table', 'tap_stream_id': 'jsonl_table', 'schema': {'type': 'object', 'properties': {'name': {'type': ['string', 'null']}, 'id': {'type': ['integer', 'string', 'null']}, 'marks': {'type': 'array', 'items': {'type': ['integer', 'string', 'null']}}, 'students': {'type': 'object', 'properties': {}}, '_sdc_source_bucket': {'type': 'string'}, '_sdc_source_file': {'type': 'string'}, '_sdc_source_lineno': {'type': 'integer'}, '_sdc_extra': {'type': 'array', 'items': {'type': 'string'}}}}, 'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'table-key-properties': ['id']}}, {'breadcrumb': ['properties', 'name'], 'metadata': {
            'inclusion': 'available'}}, {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ['properties', 'marks'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'students'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_bucket'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_file'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_lineno'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_extra'], 'metadata': {'inclusion': 'available'}}]}

        sync.sync_table_file(config, s3_path, table_spec, stream)
        self.assertEqual(mock_sync_jsonl_file.call_count, 1)

    @mock.patch("tap_s3_csv.s3.get_file_handle", side_effect=mock_json_file_handler_5_records_for_s3)
    @mock.patch("tap_s3_csv.sync.sync_csv_file", side_effect=mock_sync_csv_file)
    def test_sync_csv_file_called_in_sync_table_file_for_csv_file(self, mock_sync_csv_file, mocked_file_handle):

        s3_path = "test\\abc.csv"
        table_spec = {'table_name': 'test_table'}
        config = {'bucket': 'Test'}
        stream = {'stream': 'jsonl_table', 'tap_stream_id': 'jsonl_table', 'schema': {'type': 'object', 'properties': {'name': {'type': ['string', 'null']}, 'id': {'type': ['integer', 'string', 'null']}, 'marks': {'type': 'array', 'items': {'type': ['integer', 'string', 'null']}}, 'students': {'type': 'object', 'properties': {}}, '_sdc_source_bucket': {'type': 'string'}, '_sdc_source_file': {'type': 'string'}, '_sdc_source_lineno': {'type': 'integer'}, '_sdc_extra': {'type': 'array', 'items': {'type': 'string'}}}}, 'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'table-key-properties': ['id']}}, {'breadcrumb': ['properties', 'name'], 'metadata': {
            'inclusion': 'available'}}, {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ['properties', 'marks'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'students'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_bucket'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_file'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_lineno'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_extra'], 'metadata': {'inclusion': 'available'}}]}
        
        sync.sync_table_file(config, s3_path, table_spec, stream)
        self.assertEqual(mock_sync_csv_file.call_count, 1)

    @mock.patch("tap_s3_csv.s3.get_file_handle", side_effect=mock_json_file_handler_5_records_for_s3)
    @mock.patch("tap_s3_csv.sync.sync_csv_file", side_effect=mock_sync_csv_file)
    def test_sync_csv_file_called_in_sync_table_file_for_txt_file(self, mock_sync_csv_file, mocked_file_handle):

        s3_path = "test\\abc.txt"
        table_spec = {'table_name': 'test_table'}
        config = {'bucket': 'Test'}
        stream = {'stream': 'jsonl_table', 'tap_stream_id': 'jsonl_table', 'schema': {'type': 'object', 'properties': {'name': {'type': ['string', 'null']}, 'id': {'type': ['integer', 'string', 'null']}, 'marks': {'type': 'array', 'items': {'type': ['integer', 'string', 'null']}}, 'students': {'type': 'object', 'properties': {}}, '_sdc_source_bucket': {'type': 'string'}, '_sdc_source_file': {'type': 'string'}, '_sdc_source_lineno': {'type': 'integer'}, '_sdc_extra': {'type': 'array', 'items': {'type': 'string'}}}}, 'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'table-key-properties': ['id']}}, {'breadcrumb': ['properties', 'name'], 'metadata': {
            'inclusion': 'available'}}, {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ['properties', 'marks'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', 'students'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_bucket'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_file'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_source_lineno'], 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ['properties', '_sdc_extra'], 'metadata': {'inclusion': 'available'}}]}
        
        sync.sync_table_file(config, s3_path, table_spec, stream)
        self.assertEqual(mock_sync_csv_file.call_count, 1)

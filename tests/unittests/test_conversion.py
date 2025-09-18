import unittest
from tap_s3_csv import conversion
from unittest import mock

class TestConversion(unittest.TestCase):

    ## Tests of infer

    def test_infer_datum_equals_to_none(self):
        res = conversion.infer(None,None,[])
        self.assertEqual(res, None)

    def test_infer_datum_equals_to_empty(self):
        res = conversion.infer(None,"",[])
        self.assertEqual(res, None)

    def test_infer_datum_equals_to_datetime(self):
        res = conversion.infer("created_at","20-05-2021",["created_at"])
        self.assertEqual(res, "date-time")

    def test_infer_datum_equals_to_integer(self):
        res = conversion.infer(None,5,[])
        self.assertEqual(res, "integer")

    def test_infer_datum_equals_to_number(self):
        res = conversion.infer(None,4.5,[])
        self.assertEqual(res, "number")

    def test_infer_datum_equals_to_string(self):
        res = conversion.infer(None,"test",[])
        self.assertEqual(res, "string")

    def test_infer_datum_equals_to_dict(self):
        res = conversion.infer(None,{'test':'test'},[])
        self.assertEqual(res, "dict")

    def test_infer_datum_equals_to_list_integer(self):
        res = conversion.infer(None,[5,6],[])
        self.assertEqual(res, "list.integer")

    def test_infer_datum_equals_to_list_list(self):
        res = conversion.infer(None,[[5,6],6],[])
        self.assertEqual(res, "list.string")

    def test_infer_datum_equals_to_list_number(self):
        res = conversion.infer(None,[5.9,6.9],[])
        self.assertEqual(res, "list.number")

    def test_infer_datum_equals_to_list_string(self):
        res = conversion.infer(None,["test","test1"],[])
        self.assertEqual(res, "list.string")

    def test_infer_datum_equals_to_list_datetime(self):
        res = conversion.infer("created_at",["20-05-2021"],["created_at"])
        self.assertEqual(res, "list.date-time")

    ## Tests of pick_datatypes

    def test_pick_datatype_return_string(self):
        res = conversion.pick_datatype({ 'string' : 45})
        self.assertEqual(res, "string")

    def test_pick_datatype_return_datetime(self):
        res = conversion.pick_datatype({ 'date-time' : 5})
        self.assertEqual(res, "date-time")

    def test_pick_datatype_return_list_dict(self):
        res = conversion.pick_datatype({ 'dict' : 45})
        self.assertEqual(res, "dict")

    def test_pick_datatype_return_list_integer(self):
        res = conversion.pick_datatype({ 'list.integer' : 45})
        self.assertEqual(res, "list.integer")

    def test_pick_datatype_return_list_number(self):
        res = conversion.pick_datatype({ 'list.number' : 45})
        self.assertEqual(res, "list.number")

    def test_pick_datatype_return_list_datetime(self):
        res = conversion.pick_datatype({ 'list.date-time' : 45})
        self.assertEqual(res, "list.date-time")

    def test_pick_datatype_return_list_string(self):
        res = conversion.pick_datatype({ 'list.string' : 45})
        self.assertEqual(res, "list.string")

    def test_pick_datatype_return_integer(self):
        res = conversion.pick_datatype({ 'integer' : 45})
        self.assertEqual(res, "integer")

    def test_pick_datatype_return_number(self):
        res = conversion.pick_datatype({ 'number' : 45})
        self.assertEqual(res, "number")

    ## Tests of generate_schema

    def mock_count_sample(a,b,c):
        return {'name': {'string': 1}, 'id': {'integer': 1}, 'marks': {'list.number': 1}, 'students': {'dict': 1}, 'created_at': {'date-time': 1}, 'tota': {'list': 1}}

    @mock.patch('tap_s3_csv.conversion.count_sample',side_effect=mock_count_sample)
    def test_generate_schema(self,mock_count_sample):
        self.maxDiff = None
        samples = [{'name': 'test', 'id': 3, 'marks': [45.85, 25.38], 'students': {'no': 5, 'col': 6}, 'created_at': '20-05-2021', 'tota': []}]
        res = conversion.generate_schema2(samples)
        expected_result = {'name': {'type': ['null', 'string']}, 'id': {'type': ['null', 'integer', 'string']}, 'marks': {'anyOf': [{'type': 'array', 'items': {'type': ['null', 'number', 'string']}}, {'type': ['null', 'string']}]}, 'students': {'anyOf': [{'type': 'object', 'properties': {}}, {'type': ['null', 'string']}]}, 'created_at': {'anyOf': [{'type': ['null', 'string'], 'format': 'date-time'}, {'type': ['null', 'string']}]}, 'tota': {'anyOf': [{'type': 'array', 'items': {'type': ['null', 'string']}}, {'type': ['null', 'string']}]}}
        self.assertEqual(res, expected_result)

import unittest
from unittest import mock
from tap_s3_csv import csv

class TestRestKey(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3,4"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], ['4'])

class TestDuplicateHeaders(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5,6"]

    @mock.patch("tap_s3_csv.csv.LOGGER.warn")
    def test(self, mocked_logger):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], ['4','5','6'])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestKey(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5,6,7,8,9"]

    @mock.patch("tap_s3_csv.csv.LOGGER.warn")
    def test(self, mocked_logger):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], ['4','5','6','7','8','9'])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestValue(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5"]

    @mock.patch("tap_s3_csv.csv.LOGGER.warn")
    def test(self, mocked_logger):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], ['4','5'])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestValueNoSDCExtra(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3"]

    @mock.patch("tap_s3_csv.csv.LOGGER.warn")
    def test(self, mocked_logger):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC"])

class TestNullBytes(unittest.TestCase):

    csv_data = [b"columnA,columnB\0,columnC", b"1,2,3,4"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

class TestWarningForDupHeaders(unittest.TestCase):
    
    csv_data = [b"columnA,columnB,columnC,columnC", b"1,2,3"]

    @mock.patch("tap_s3_csv.csv.LOGGER.warn")
    def test(self, mocked_logger):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC"])

        mocked_logger.assert_called_with('Duplicate Header(s) %s found in the csv and its value will be stored in the \"_sdc_extra\" field.', {'columnC'})

class TestOptions(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        with self.assertRaisesRegex(Exception, "CSV file missing required headers: {'fizz'}"):
            row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['fizz']})

        row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        with self.assertRaisesRegex(Exception, "CSV file missing date_overrides headers: {'fizz'}"):
            row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['fizz']})

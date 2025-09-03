import unittest
import os
import json
import io
import gzip
import singer
import zipfile
from unittest import mock
from tap_s3_csv import s3
from singer_encodings import csv
from tap_s3_csv import sync
from tap_s3_csv import utils

COMPRESSION_FOLDER_PATH = "Compressed"
CSV_FOLDER_PATH = "Compressed-CSV"
JSONL_FOLDER_PATH = "Compressed-JSONL"

def get_resources_path(file_path, folder_path=None):
    if folder_path:
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources', folder_path, file_path)
    else:
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources', file_path)


class ZipFileMocked(zipfile.ZipFile):
    def __init__(self, name):
        self.name = name


def mockclass():
    class Transformer():
        def __init__(self):
            pass
        def transform(self,rec,schema,matadata):
            return rec
    return Transformer()


class TestCompressedFileSupport(unittest.TestCase):

    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_s3_bucket_key(self, mocked_get_file_handle):
        config = {}

        sample_key = { "other_key" : "unittest_compressed_files/sample.txt" }

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertListEqual([], files)


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    @mock.patch("singer_encodings.compression.infer")
    def test_sampling_of_zip_file(self, mocked_infer, mocked_get_file_handle):
        config = {}

        sample_key = { "key" : "unittest_compressed_files/sample_compressed_zip_mixer_files.zip" }


        zip_file_path = get_resources_path("sample_compressed_zip_mixer_files.zip", CSV_FOLDER_PATH)

        expected_extensions = ["csv","gz","jsonl"]

        with zipfile.ZipFile(zip_file_path, "r") as zip_file:

            mocked_get_file_handle.return_value = zip_file.fp
            mocked_infer.return_value = [zip_file.open(file) for file in zip_file.namelist()]
            files = s3.get_files_to_sample(config, [sample_key], 5)

            self.assertTrue(all([True for file in files if file["file_handle"].name.split(".")[-1].lower() in expected_extensions]))


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_non_compress_file_csv(self, mocked_get_file_handle):
        config = {}

        sample_key = { "key" : "unittest_compressed_files/sample.csv" }

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertTrue("type" not in files[0])


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_breaking_loop_at_reaching_max_count(self, mocked_get_file_handle):
        config = {}
        max_files = 5
        sample_keys = [
            { "key" : "a.jsonl" },
            { "key" : "b.csv" },
            { "key" : "c.gz" },
            { "key" : "d.txt" },
            { "key" : "e.jsonl" },
        ]

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, sample_keys, max_files)

        self.assertEqual(max_files, len(files))


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_non_compress_file_jsonl(self, mocked_get_file_handle):
        config = {}

        sample_key = { "key" : "unittest_compressed_files/sample.jsonl" }

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertTrue("type" not in files[0])


    def test_csv_records_as_samples(self):
        table_spec = {}
        file_handle = [
            b"columnA,columnB,columnC",
            b"1,2,3,4"
        ]
        s3_path = "unittest_compressed_files/sample.csv"
        sample_rate = 5
        extension = "csv"

        expected_output = [
            {"columnA" : "1", "columnB" : "2", "columnC" : "3"}
        ]

        actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

        self.assertEqual(expected_output, actual_output)


    def test_csv_records(self):
        table_spec = {}
        file_handle = [
            b"columnA,columnB,columnC",
            b"1,2,3",
            b"1,2,3",
            b"1,2,3",
            b"1,2,3",
            b"1,2,3",
            b"4,5,6"
        ]
        s3_path = "unittest_compressed_files/sample.csv"

        iterator = csv.get_row_iterator(file_handle, table_spec)

        expected_output = [
            {"columnA" : "1", "columnB" : "2", "columnC" : "3"},{"columnA" : "4", "columnB" : "5", "columnC" : "6"}
        ]

        actual_output = [record for record in s3.get_records_for_csv(s3_path, 5, iterator)]

        self.assertEqual(expected_output, actual_output)


    def test_gz_samples_for_csv(self):
        gz_file_path = get_resources_path("sample_compressed_gz_file_with_csv_file_2_records.gz",CSV_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            table_spec = {}
            file_handle = gz_file.fileobj
            s3_path = "unittest_compressed_files/sample_compressed_gz_file_with_csv_file_2_records.gz"
            sample_rate = 5
            extension = "gz"

            expected_output = [
                {'id': '1', 'location': 'Eldon Base for stackable storage shelf, platinum', 'name': 'Muhammed MacIntyre', 'count': '3', 'decimal1': '-213.25', 'decimal2': '38.94', 'decimal3': '35', 'category': 'Nunavut', 'point': 'Storage & Organization'}
            ]

            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

            self.assertEqual(expected_output, actual_output)


    def test_gz_samples_for_jsonl(self):
        gz_file_path = get_resources_path("sample_compressed_gz_file_with_json_file_2_records.gz",JSONL_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            table_spec = {}
            file_handle = gz_file.fileobj
            s3_path = "unittest_compressed_files/sample_compressed_gz_file_with_json_file_2_records.gz"
            sample_rate = 5
            extension = "gz"

            expected_output = [
                {"id":1,"name":"abc","semester":1,"created_at":"2021-05-21"}]

            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

            self.assertEqual(expected_output, actual_output)


    def test_gz_inner_filename(self):
        gz_file_path = get_resources_path("sample_compressed_gz_file_with_csv_file_2_records.gz",CSV_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            expected_output = "sample_csv_file_2_records_01.csv"
            actual_output = utils.get_file_name_from_gzfile(fileobj=gz_file.fileobj)

            self.assertEqual(expected_output, actual_output)


    @mock.patch("tap_s3_csv.sync.sync_compressed_file")
    def test_syncing_of_compressed_zip_file(self, mocked_sync_compressed_file):
        mocked_sync_compressed_file.return_value = 0
        config = {}
        table_spec = {}
        stream = {}
        s3_path = "csv_files.zip"
        sync.sync_table_file(config, s3_path, table_spec, stream)

        mocked_sync_compressed_file.assert_called_with(config, s3_path, table_spec, stream)


    @mock.patch("tap_s3_csv.sync.handle_file")
    def test_syncing_of_compressed_gz_file(self, mocked_handle_file):
        mocked_handle_file.return_value = 0
        config = {}
        table_spec = {}
        stream = {}
        s3_path = "csv_files.gz"
        sync.sync_table_file(config, s3_path, table_spec, stream)

        mocked_handle_file.assert_called_with(config, s3_path, table_spec, stream, "gz")


@mock.patch("tap_s3_csv.sync.LOGGER.warning")
class TestUnsupportedFiles(unittest.TestCase):

    def mock_get_files_to_sample_csv(config, s3_files, max_files):
        gz_file_path = get_resources_path("gz_stored_as_csv.csv", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            
            file_handle = gz_file.fileobj

            return [{'s3_path': 'unittest_compressed_files/gz_stored_as_csv.csv', 'file_handle': file_handle, 'extension': 'csv'}]

    def mock_csv_sample_file(table_spec, s3_path, file_handle, sample_rate, extension):
        raise UnicodeDecodeError("test",b"'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte",42, 43, 'the universe and everything else')
    
    def mock_get_files_to_sample_jsonl(config, s3_files, max_files):
        gz_file_path = get_resources_path("gz_stored_as_jsonl.jsonl", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            
            file_handle = gz_file.fileobj

            return [{'s3_path': 'unittest_compressed_files/gz_stored_as_jsonl.jsonl', 'file_handle': file_handle, 'extension': 'jsonl'}]

    def mock_jsonl_sample_file(table_spec, s3_path, file_handle, sample_rate, extension):
        # To raise json decoder error.
        return json.loads(b"'{'}")

    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_get_files_for_samples_of_tar_gz_file_samples(self, mocked_file_handle, mocked_logger):
        config = {}
        sample_key = { "key" : "unittest_compressed_files/sample_compressed.tar.gz" }
        mocked_file_handle.return_value = None

        actual_output = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertEqual(0,len(actual_output))

        mocked_logger.assert_called_with('Skipping "%s" file as .tar.gz extension is not supported',sample_key["key"])

    
    @mock.patch("singer_encodings.compression.infer")
    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_get_files_for_samples_of_zip_contains_tar_gz_file(self, mocked_file_handle, mocked_infer, mocked_logger):
        config = {}
        sample_key = { "key" : "unittest_compressed_files/sample_compressed.zip" }
        mocked_file_handle.return_value = None

        zip_file_path = get_resources_path("sample_compressed_zip_contains_tar_gz_file.zip", COMPRESSION_FOLDER_PATH)
        with zipfile.ZipFile(zip_file_path, "r") as zip_file:

            mocked_file_handle.return_value = zip_file.fp
            mocked_infer.return_value = [zip_file.open(file) for file in zip_file.namelist()]
            actual_output = s3.get_files_to_sample(config, [sample_key], 5)
            self.assertEqual(0,len(actual_output))


    def test_sampling_of_tar_gz_file_samples(self, mocked_logger):
        table_spec = {}
        file_handle = None
        s3_path = "unittest_compressed_files/sample_compressed.tar.gz"
        sample_rate = 5
        extension = "gz"

        actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

        self.assertTrue(len(actual_output)==0)

        mocked_logger.assert_called_with('Skipping "%s" file as .tar.gz extension is not supported',s3_path)


    def test_sampling_of_unsupported_file_samples(self, mocked_logger):
        table_spec = {}
        file_handle = None
        s3_path = "unittest_compressed_files/sample_compressed.exe"
        sample_rate = 5
        extension = "exe"

        actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

        self.assertTrue(len(actual_output)==0)

        mocked_logger.assert_called_with('"%s" having the ".%s" extension will not be sampled.',s3_path,extension)


    def test_sampling_of_file_without_extension_samples(self, mocked_logger):
        table_spec = {}
        file_handle = None
        s3_path = "unittest_compressed_files/sample_compressed"
        sample_rate = 5
        extension = s3_path.split(".")[-1].lower()

        actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, file_handle, sample_rate, extension)]

        self.assertTrue(len(actual_output)==0)

        mocked_logger.assert_called_with('"%s" without extension will not be sampled.',s3_path)


    def test_sampling_of_file_gzip_using_no_name(self, mocked_logger):
        table_spec = {}
        s3_path = "unittest_compressed_files/sample_compressed.gz"
        sample_rate = 5
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_no_name.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, gz_file.fileobj, sample_rate, extension)]

            self.assertTrue(len(actual_output)==0)

            mocked_logger.assert_called_with('Skipping "%s" file as we did not get the original file name',s3_path)


    def test_sampling_of_gz_file_contains_gz_file_samples(self, mocked_logger):
        table_spec = {}
        s3_path = "unittest_compressed_files/sample_compressed_gz_file.gz"
        sample_rate = 5
        extension = s3_path.split(".")[-1].lower()

        gz_file_path = get_resources_path("sample_compressed_gz_file_contains_gz.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            
            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, gz_file.fileobj, sample_rate, extension)]

            self.assertTrue(len(actual_output)==0)

            mocked_logger.assert_called_with('Skipping "%s" file as it contains nested compression.',s3_path)

    def test_sampling_of_empty_csv_converted_to_gz(self, mocked_logger):
        table_spec = {}
        s3_path = "unittest_compressed_files/empty_csv_gz.gz"
        sample_rate = 5
        extension = s3_path.split(".")[-1].lower()

        gz_file_path = get_resources_path("empty_csv_gz.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            
            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, gz_file.fileobj, sample_rate, extension)]

            self.assertTrue(len(actual_output)==0)

            new_s3_path = "unittest_compressed_files/empty_csv_gz.gz/empty_csv.csv"

            mocked_logger.assert_called_with('Skipping "%s" file as it is empty',new_s3_path)

    @mock.patch("tap_s3_csv.s3.get_files_to_sample",side_effect=mock_get_files_to_sample_csv)
    @mock.patch("tap_s3_csv.s3.get_file_handle")
    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_csv_sample_file)
    def test_sampling_of_gz_file_stored_with_csv_Extension(self, mock_csv_sample_file, mock_get_file_handle, mock_get_files_to_sample_csv, mocked_logger):
        table_spec = {}
        s3_files = "unittest_compressed_files/gz_stored_as_csv.csv"
        sample_rate = 5
        config = []


        actual_output = [sample for sample in s3.sample_files(config, table_spec, s3_files, sample_rate)]

        self.assertTrue(len(actual_output)==0)

        new_s3_path = "unittest_compressed_files/gz_stored_as_csv.csv"

        mocked_logger.assert_called_with('Skipping %s file as parsing failed. Verify an extension of the file.',new_s3_path)

    @mock.patch("tap_s3_csv.s3.get_files_to_sample",side_effect=mock_get_files_to_sample_jsonl)
    @mock.patch("tap_s3_csv.s3.get_file_handle")
    @mock.patch("tap_s3_csv.s3.sample_file", side_effect=mock_jsonl_sample_file)
    def test_sampling_of_gz_file_stored_with_jsonl_Extension(self, mock_jsonl_sample_file, mock_get_file_handle, mock_get_files_to_sample_csv, mocked_logger):
        table_spec = {}
        s3_files = "unittest_compressed_files/gz_stored_as_jsonl.jsonl"
        sample_rate = 5
        config = []


        actual_output = [sample for sample in s3.sample_files(config, table_spec, s3_files, sample_rate)]

        self.assertTrue(len(actual_output)==0)

        new_s3_path = "unittest_compressed_files/gz_stored_as_jsonl.jsonl"

        mocked_logger.assert_called_with('Skipping %s file as parsing failed. Verify an extension of the file.',new_s3_path)

    def test_sampling_of_gz_file_contains_zip_file_samples(self, mocked_logger):
        table_spec = {}
        s3_path = "unittest_compressed_files/sample_compressed_gz_file_contains_zip.gz"
        sample_rate = 5
        extension = s3_path.split(".")[-1].lower()

        gz_file_path = get_resources_path("sample_compressed_gz_file_contains_zip.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:
            
            actual_output = [sample for sample in s3.sample_file(table_spec, s3_path, gz_file.fileobj, sample_rate, extension)]

            self.assertTrue(len(actual_output)==0)

            new_s3_path = "unittest_compressed_files/sample_compressed_gz_file_contains_zip.gz/csv_jsonl.zip"

            mocked_logger.assert_called_with('Skipping "%s" file as it contains nested compression.',new_s3_path)


    @mock.patch("tap_s3_csv.utils.get_file_name_from_gzfile")
    def test_sampling_of_error_gz_file_samples(self, mocked_gz_file_name,mocked_logger):
        table_spec = {}
        s3_path = "unittest_compressed_files/sample_compressed_gz_file.gz"
        sample_rate = 5
        extension = s3_path.split(".")[-1].lower()

        gz_file_path = get_resources_path("sample_compressed_gz_file.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            mocked_gz_file_name.return_value = None

            try:
                s3.sample_file(table_spec, s3_path, gz_file.fileobj, sample_rate, extension)
            except Exception as e:
                expected_message = '"{}" file has some error(s)'.format(s3_path)
                self.assertEqual(expected_message, str(e))
            


    def test_syncing_of_unsupported_file(self, mocked_logger):
        config = {}
        table_spec = {}
        stream = {}
        s3_path = "csv_files.exe"
        extension = "exe"
        records = sync.sync_table_file(config, s3_path, table_spec, stream)

        mocked_logger.assert_called_with('"%s" having the ".%s" extension will not be synced.',s3_path,extension)
        self.assertEqual(0, records)


    def test_syncing_of_file_without_extension(self, mocked_logger):
        config = {}
        table_spec = {}
        stream = {}
        s3_path = "unittest_compressed_files/sample"
        records = sync.sync_table_file(config, s3_path, table_spec, stream)

        self.assertEqual(0, records)

        mocked_logger.assert_called_with('"%s" without extension will not be synced.',s3_path)


    def test_syncing_records_of_file_without_extension(self, mocked_logger):
        config = {}
        table_spec = {}
        s3_path = "unittest_compressed_files/sample"

        extension = s3_path.split(".")[-1].lower()

        records = sync.handle_file(config, s3_path, table_spec, {}, extension)

        self.assertEqual(0, records)

        mocked_logger.assert_called_with('"%s" without extension will not be synced.',s3_path)


    def test_syncing_of_unsupported_file_records(self, mocked_logger):
        config = {}
        table_spec = {}
        s3_path = "unittest_compressed_files/sample.exe"

        extension = s3_path.split(".")[-1].lower()

        records = sync.handle_file(config, s3_path, table_spec, {}, extension)

        self.assertEqual(0, records)

        mocked_logger.assert_called_with('"%s" having the ".%s" extension will not be synced.',s3_path,extension)

    @mock.patch("tap_s3_csv.sync.handle_file", side_effect=mock_csv_sample_file)
    def test_syncing_of_gz_file_with_csv_extension(self, mock_csv_sample_file,mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}
        stream = {}
        s3_path = "unittest_compressed_files/gz_stored_as_csv.csv"
        extension = "csv"
        records = sync.sync_table_file(config, s3_path, table_spec, stream)

        mocked_logger.assert_called_with('Skipping %s file as parsing failed. Verify an extension of the file.',s3_path)
        self.assertEqual(0, records)

    @mock.patch("tap_s3_csv.sync.handle_file", side_effect=mock_jsonl_sample_file)
    def test_syncing_of_gz_file_with_jsonl_extension(self, mock_jsonl_sample_file,mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}
        stream = {}
        s3_path = "unittest_compressed_files/gz_stored_as_jsonl.jsonl"
        extension = "jsonl"
        records = sync.sync_table_file(config, s3_path, table_spec, stream)

        mocked_logger.assert_called_with('Skipping %s file as parsing failed. Verify an extension of the file.',s3_path)
        self.assertEqual(0, records)

    def test_syncing_tar_gz_file(self, mocked_logger):
        config = {}
        table_spec = {}
        s3_path = "unittest_compressed_files/sample_compressed.tar.gz"
        extension = "gz"

        records = sync.handle_file(config, s3_path, table_spec, {}, extension)

        self.assertTrue(records == 0)

        mocked_logger.assert_called_with('Skipping "%s" file as .tar.gz extension is not supported',s3_path)

    def test_syncing_gz_file_of_file_gzip_using_no_name(self, mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}
        s3_path = "unittest_compressed_files/sample_compressed.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_no_name.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            records = sync.handle_file(config, s3_path, table_spec, {}, extension, gz_file.fileobj)

            self.assertEqual(records, 0)

            mocked_logger.assert_called_with('Skipping "%s" file as we did not get the original file name',s3_path)


    def test_syncing_gz_file_contains_gz(self, mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}

        s3_path = "unittest_compressed_files/sample_compressed_gz_file_contains_gz.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_gz_file_contains_gz.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            records = sync.handle_file(config, s3_path, table_spec, {}, extension, gz_file.fileobj)

            mocked_logger.assert_called_with('Skipping "%s" file as it contains nested compression.',s3_path)
            
            self.assertEqual(records, 0)

    def test_syncing_gz_file_contains_zip(self, mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}

        s3_path = "unittest_compressed_files/sample_compressed_gz_file_contains_zip.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_gz_file_contains_zip.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            records = sync.handle_file(config, s3_path, table_spec, {}, extension, gz_file.fileobj)

            new_s3_path = "unittest_compressed_files/sample_compressed_gz_file_contains_zip.gz/csv_jsonl.zip"

            mocked_logger.assert_called_with('Skipping "%s" file as it contains nested compression.',new_s3_path)
            
            self.assertEqual(records, 0)


    @mock.patch("tap_s3_csv.utils.get_file_name_from_gzfile")
    def test_syncing_of_error_gz_file(self, mocked_gz_file_name,mocked_logger):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_DATA"}

        s3_path = "unittest_compressed_files/sample_compressed_gz_file.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_gz_file.gz", COMPRESSION_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            mocked_gz_file_name.return_value = None

            try:
                sync.handle_file(config, s3_path, table_spec, {}, extension, gz_file.fileobj)
            except Exception as e:
                expected_message = '"{}" file has some error(s)'.format(s3_path)
                self.assertEqual(expected_message, str(e))


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_get_sampling_files_with_file_without_extension(self, mocked_get_file_handle, mocked_logger):
        config = {}

        sample_key = { "key" : "unittest_compressed_files/sample" }

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertTrue(len(files) == 0)

        mocked_logger.assert_called_with('"%s" without extension will not be sampled.',sample_key["key"])


    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_get_sampling_files_with_unsupported_file(self, mocked_get_file_handle, mocked_logger):
        config = {}

        sample_key = { "key" : "unittest_compressed_files/sample.exe" }

        extension = sample_key["key"].split(".")[-1].lower()

        mocked_get_file_handle.return_value = None

        files = s3.get_files_to_sample(config, [sample_key], 5)

        self.assertTrue(len(files) == 0)

        mocked_logger.assert_called_with('"%s" having the ".%s" extension will not be sampled.',sample_key["key"],extension)


@mock.patch("singer.Transformer",side_effect=mockclass)
@mock.patch("singer.write_record")
class TestSyncingCompressedFiles(unittest.TestCase):


    def test_syncing_gz_file_contains_csv(self, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_CSV_DATA"}

        catalog_path = get_resources_path("sample_csv_catalog.json", CSV_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_compressed_gz_file.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_gz_file.gz", CSV_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            records = sync.handle_file(config, s3_path, table_spec, stream, extension, gz_file.fileobj)

            self.assertTrue(records == 998)


    def test_syncing_gz_file_contains_jsonl(self, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "GZ_JSONL_DATA"}

        catalog_path = get_resources_path("sample_jsonl_catalog.json", JSONL_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_compressed_gz_file_with_json_file_2_records.gz"
        extension = "gz"

        gz_file_path = get_resources_path("sample_compressed_gz_file_with_json_file_2_records.gz", JSONL_FOLDER_PATH)

        with gzip.GzipFile(gz_file_path) as gz_file:

            records = sync.handle_file(config, s3_path, table_spec, stream, extension, gz_file.fileobj)

            self.assertTrue(records == 2)


    def test_syncing_csv_file(self, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "CSV_DATA"}

        catalog_path = get_resources_path("sample_csv_catalog.json", CSV_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_csv_file_01.csv"
        extension = "csv"

        csv_file_path = get_resources_path("sample_csv_file_01.csv", CSV_FOLDER_PATH)

        with open(csv_file_path, "rb") as csv_file:

            records = sync.handle_file(config, s3_path, table_spec, stream, extension, csv_file)

            self.assertTrue(records == 998)
    

    def test_syncing_jsonl_file(self, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "JSONL_DATA"}

        catalog_path = get_resources_path("sample_jsonl_catalog.json", JSONL_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_jsonl_file.jsonl"
        extension = "jsonl"

        jsonl_file_path = get_resources_path("sample_jsonl_file.jsonl", JSONL_FOLDER_PATH)

        with open(jsonl_file_path, "rb") as jsonl_file:

            records = sync.handle_file(config, s3_path, table_spec, stream, extension, jsonl_file)

            self.assertTrue(records == 10)


    @mock.patch("singer_encodings.compression.infer")
    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_syncing_zip_file_for_csv(self, mocked_file_handle, mocked_infer, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "ZIP_DATA"}

        catalog_path = get_resources_path("sample_csv_catalog.json", CSV_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_compressed_zip_mixer_files.zip"

        zip_file_path = get_resources_path("sample_compressed_zip_mixer_files.zip", CSV_FOLDER_PATH)

        with zipfile.ZipFile(zip_file_path, "r") as zip_file:

            mocked_file_handle.return_value = zip_file.fp
            mocked_infer.return_value = [zip_file.open(file) for file in zip_file.namelist()]
            records = sync.sync_compressed_file(config, s3_path, table_spec, stream)

            self.assertTrue(records == 1983)


    @mock.patch("singer_encodings.compression.infer")
    @mock.patch("tap_s3_csv.s3.get_file_handle")
    def test_syncing_zip_file_for_jsonl(self, mocked_file_handle, mocked_infer, mocked_write_record, mock_class):
        config = {"bucket" : "bucket_name"}
        table_spec = { "table_name" : "ZIP_DATA"}

        catalog_path = get_resources_path("sample_jsonl_catalog.json", JSONL_FOLDER_PATH)
        catalog_file = open(catalog_path, "r")
        stream = json.load(catalog_file)

        s3_path = "unittest_compressed_files/sample_compressed_zip_mixer_files.zip"

        zip_file_path = get_resources_path("sample_compressed_zip_mixer_files.zip", JSONL_FOLDER_PATH)

        with zipfile.ZipFile(zip_file_path, "r") as zip_file:

            mocked_file_handle.return_value = zip_file.fp
            mocked_infer.return_value = [zip_file.open(file) for file in zip_file.namelist()]
            records = sync.sync_compressed_file(config, s3_path, table_spec, stream)

            self.assertTrue(records == 4)

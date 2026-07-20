import unittest
import tempfile
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from tap_s3_csv import s3


def make_parquet_file(num_rows, row_group_size, compression='snappy'):
    parquet_file = tempfile.TemporaryFile('w+b')
    data = {
        "id": list(range(1, num_rows + 1)),
        "name": [f"user_{i}" for i in range(1, num_rows + 1)],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file, row_group_size=row_group_size, compression=compression)
    parquet_file.seek(0)
    return parquet_file


class TestGetRecordsForParquet(unittest.TestCase):
    """
    Confirms the tap's Parquet sampling is wired up to
    singer_encodings.parquet.sample_row_iterator() (which filters rows
    before converting them to Python objects) instead of the old
    get_row_iterator() + post-hoc filtering approach, and that logging
    behavior is preserved.
    """

    def tearDown(self):
        self.parquet_file.close()

    def test_samples_rows_at_the_configured_rate(self):
        # 10 row groups of 10 rows each.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        records = list(s3.get_records_for_parquet(
            "s3://bucket/file.parquet", sample_rate=10, max_records=1000, file_handle=self.parquet_file))

        self.assertEqual([r['id'] for r in records], list(range(1, 101, 10)))

    def test_stops_early_without_reading_every_row_group(self):
        # 10 row groups of 10 rows each; asking for only 3 sampled rows at
        # sample_rate=1 should mean only the first row group ever gets
        # decompressed via read_row_group.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)
        original_read_row_group = pq.ParquetFile.read_row_group

        with mock.patch.object(pq.ParquetFile, 'read_row_group', autospec=True) as mocked_read:
            mocked_read.side_effect = original_read_row_group
            records = list(s3.get_records_for_parquet(
                "s3://bucket/file.parquet", sample_rate=1, max_records=3, file_handle=self.parquet_file))

        self.assertEqual([r['id'] for r in records], [1, 2, 3])
        self.assertEqual(mocked_read.call_count, 1)

    @mock.patch("tap_s3_csv.s3.LOGGER")
    def test_logs_progress_every_200_sampled_rows(self, mocked_logger):
        self.parquet_file = make_parquet_file(num_rows=500, row_group_size=50)

        records = list(s3.get_records_for_parquet(
            "s3://bucket/file.parquet", sample_rate=1, max_records=1000, file_handle=self.parquet_file))

        self.assertEqual(len(records), 500)
        mocked_logger.info.assert_any_call("Sampled %s rows from %s", 200, "s3://bucket/file.parquet")
        mocked_logger.info.assert_any_call("Sampled %s rows from %s", 400, "s3://bucket/file.parquet")
        # Final summary log after the loop completes.
        mocked_logger.info.assert_any_call("Sampled %s rows from %s", 500, "s3://bucket/file.parquet")

    def test_sample_file_dispatches_parquet_extension_to_get_records_for_parquet(self):
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        records = list(s3.sample_file({}, "s3://bucket/file.parquet", self.parquet_file, sample_rate=5, extension="parquet", max_records=1000))

        self.assertEqual([r['id'] for r in records], list(range(1, 101, 5)))

    @mock.patch("tap_s3_csv.s3.LOGGER")
    def test_sample_file_warns_and_skips_an_empty_parquet_file(self, mocked_logger):
        self.parquet_file = make_parquet_file(num_rows=0, row_group_size=None)
        skipped_before = s3.skipped_files_count

        records = list(s3.sample_file({}, "s3://bucket/empty.parquet", self.parquet_file, sample_rate=5, extension="parquet", max_records=1000))

        self.assertEqual(records, [])
        mocked_logger.warning.assert_any_call('Skipping "%s" file as it is empty', "s3://bucket/empty.parquet")
        self.assertEqual(s3.skipped_files_count, skipped_before + 1)

    def test_samples_a_snappy_compressed_parquet_file(self):
        # Snappy is pyarrow's default Parquet compression codec, so every
        # other test in this file already exercises it implicitly. This
        # test makes that coverage explicit end-to-end through the tap's
        # sample_file() dispatch, and confirms the codec really is SNAPPY.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10, compression='snappy')

        pf = pq.ParquetFile(self.parquet_file)
        self.assertEqual(pf.metadata.row_group(0).column(0).compression, 'SNAPPY')
        self.parquet_file.seek(0)

        records = list(s3.sample_file({}, "s3://bucket/file.parquet", self.parquet_file, sample_rate=5, extension="parquet", max_records=1000))

        self.assertEqual([r['id'] for r in records], list(range(1, 101, 5)))

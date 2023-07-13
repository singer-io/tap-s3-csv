from queue import Queue
import csv
import codecs
from tap_s3_csv import s3
from tap_s3_csv.symon_exception import SymonException

# Wrapper class for file streams. Handles preprocessing (skipping header rows, footer rows, detecting headers)
class PreprocessStream():
    def __init__(self, file_handle, table_spec, handle_first_row, s3_path=None, config=None):
        self.file_iterator = file_handle.iter_lines()
        self.first_row = None
        self.queue = None
        self.header = None
        self.skip_header_row = table_spec.get('skip_header_row', 0)

        skip_footer_row = table_spec.get('skip_footer_row', 0)

        self._skip_header_rows()
        if skip_footer_row > 0:
            self.queue = Queue(maxsize=skip_footer_row)
        if handle_first_row:
            self._handle_first_row(table_spec, s3_path, config)

    def _skip_header_rows(self):
        try:
            for _ in range(self.skip_header_row):
                next(self.file_iterator)
        except StopIteration:
            raise SymonException(
                f"We can't find any data after the skipped rows in the header. Please check skip/ignore configuration.", 'PreprocessError')

    # skips empty rows and process first non-empty row as header row or first record row depending on has_header
    def _handle_first_row(self, table_spec, s3_path=None, config=None):
        has_header = table_spec.get('has_header', True)
        first_row_parsed = self._get_first_row(table_spec)

        # first row is header row
        if has_header:
            self.header = first_row_parsed
            return

        # first row is a record, generate headers
        self.header = [f'col_{i}' for i in range(len(first_row_parsed))]
        # first row has been iterated already, reset file handle so that we don't lose first row and yield it
        if s3_path is not None and config is not None:
            self._reset_file_iterator(s3_path, config)

    # resets file_handle and skips header rows
    def _reset_file_iterator(self, s3_path, config):
        file_handle = s3.get_file_handle(config, s3_path)
        self.file_iterator = file_handle.iter_lines()
        self._skip_header_rows()

    # grabs first non empty row using csv.DictReader
    def _get_first_row(self, table_spec):
        encoding = table_spec.get('encoding', 'utf-8')
        delimiter = table_spec.get('delimiter', ',')
        quotechar = table_spec.get('quotechar', '"')
        escapechar = table_spec.get('escape_char', '\\')

        # csv.DictReader automatically skips empty rows and grabs the first row as header and saves it in it's fieldnames property
        # if fieldnames passed in is None. Use csv.DictReader to grab the first row as it handles corner cases for row such as:
        # - fields in first row contain newline char wrapped with quotechar or escaped with escapechar
        # - fields in first row contain delimiter wrapped with quotechar or escaped with escapechar
        file_stream = codecs.iterdecode(
            self.file_iterator, encoding=encoding, errors='replace')
        reader = csv.DictReader(
            (line.replace('\0', '') for line in file_stream),
            fieldnames=None,
            delimiter=delimiter,
            escapechar=escapechar,
            quotechar=quotechar)

        if reader.fieldnames is None:
            raise SymonException(
                "We can't find any data. Please check skip/ignore configuration.", 'PreprocessError')

        return reader.fieldnames

    def iter_lines(self):
        for row in self.file_iterator:
            if self.queue is None:
                yield row
            else:
                if self.queue.full():
                    yield self.queue.get()
                self.queue.put(row)

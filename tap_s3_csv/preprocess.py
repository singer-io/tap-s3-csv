from queue import Queue

# Wrapper class for file streams. Handles preprocessing (skipping header rows, footer rows, detecting headers)
class PreprocessStream():
    def __init__(self, file_handle, table_spec, handle_first_row):
        self.file_iterator = file_handle.iter_lines()
        self.first_row = None
        self.queue = None
        self.header = None

        skip_header_row = table_spec.get('skip_header_row', 0)
        skip_footer_row = table_spec.get('skip_footer_row', 0)

        if skip_header_row > 0:
            self._skip_header_rows(skip_header_row)
        if skip_footer_row > 0:
            self.queue = Queue(maxsize = skip_footer_row)
        if handle_first_row:
            has_header = table_spec.get('has_header', True)
            encoding = table_spec.get('encoding', 'utf-8')
            delimiter = table_spec.get('delimiter', ',')
            self._handle_first_row(has_header, encoding, delimiter)
    
    def _skip_header_rows(self, skip_header_row):
        try:
            for _ in range(skip_header_row):
                next(self.file_iterator)
        except StopIteration:
            raise Exception(f'preprocess_err: No more data after skipping rows in header.')

    def _skip_empty_rows(self):
        try:
            first_row = next(self.file_iterator)
            while first_row == b'' or first_row == b'\n':
                first_row = next(self.file_iterator)
        except StopIteration:
            raise Exception(f'preprocess_err: No more data other than empty rows.')
        return first_row
    
    # grabs first non empty row and process it as header row or first record row depending on has_header
    def _handle_first_row(self, has_header, encoding, delimiter):
        first_row = self._skip_empty_rows()
        first_row_list = first_row.decode(encoding).split(delimiter)
        
        # first row is header row
        if has_header:
            self.header = first_row_list
            return
        
        # first row is a record, generate headers and store first row so we can yield it in iter_lines
        self.first_row = first_row
        fieldnames = [f'col_{i}' for i in range(len(first_row_list))]
        self.header = fieldnames

    def iter_lines(self):
        if self.first_row is not None:
            if self.queue is None:
                yield self.first_row
                self.first_row = None
            else:
                self.queue.put(self.first_row)
        
        for row in self.file_iterator:
            if self.queue is None:
                yield row
            else:
                if self.queue.full():
                    yield self.queue.get()
                self.queue.put(row)

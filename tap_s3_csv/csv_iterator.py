import codecs
import csv


def get_row_iterator(iterable, options=None):
    options = options or {}
    file_stream = codecs.iterdecode(
        iterable.iter_lines(), encoding=options.get('encoding', 'utf-8'), errors='replace')

    field_names = None

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader(
        (line.replace('\0', '') for line in file_stream),
        fieldnames=field_names,
        delimiter=options.get('delimiter', ','),
        escapechar=options.get('escape_char', '\\'),
        quotechar=options.get('quotechar', '"'))

    headers = set(reader.fieldnames)

    # Check for duplicate columns
    fieldname_pool = set()
    duplicate_cols = set()
    if len(reader.fieldnames) != len(headers):
        for fieldname in reader.fieldnames:
            if fieldname == '':
                continue
            fieldname = fieldname.casefold()
            if fieldname in fieldname_pool:
                duplicate_cols.add(fieldname)
            else:
                fieldname_pool.add(fieldname)

        if len(duplicate_cols) > 0:
            raise Exception(
                'CSV file contains duplicate columns: {}'.format(duplicate_cols))

    reader.fieldnames = handle_empty_fieldnames(
        reader.fieldnames, fieldname_pool, options)

    # csv.DictReader skips empty rows, but we wish to keep empty rows for csv imports, so override __next__ method.
    # Only modifying for imports from csv connector for now as imports from s3 connector might have reasons for skipping empty rows.
    # Could look into using csv.reader instead for cleaner code if s3 connector could also keep empty rows.
    if options.get('is_csv_connector_import', False):
        csv.DictReader.__next__ = next_without_skip

    # We do not use key_properties and date_overrides in our config. If we use these later, will need to add code for checking over
    # whether fieldnames included in key_properties/date_overrides have been modified in handle_empty_fieldnames and handle appropriately.
    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('CSV file missing required headers: {}, file only contains headers for fields: {}'
                            .format(key_properties - headers, headers))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('CSV file missing date_overrides headers: {}, file only contains headers for fields: {}'
                            .format(date_overrides - headers, headers))

    return reader

# Generates column name for columns without header


def handle_empty_fieldnames(fieldnames, fieldname_pool, options):
    quotechar = options.get('quotechar', '"')
    delimiter = options.get('delimiter', ',')
    is_csv_connector_import = options.get('is_csv_connector_import',  False)

    auto_generate_header_num = 0
    final_fieldnames = []
    for fieldname in fieldnames:
        # handle edge case uncovered in WP-9886 for csv import
        if is_csv_connector_import and fieldname and delimiter in fieldname:
            fieldname = quotechar + fieldname + quotechar

        if fieldname == '' or fieldname is None:
            fieldname = f'col_{auto_generate_header_num}'
            while fieldname in fieldname_pool:
                auto_generate_header_num += 1
                fieldname = f'col_{auto_generate_header_num}'
            auto_generate_header_num += 1

        final_fieldnames.append(fieldname)

    return final_fieldnames

# csv.DictReader class skips empty rows when iterating over file stream.
# method to use for overriding csv.DictReader.__next__ in case we wish to keep empty rows


def next_without_skip(self):
    if self.line_num == 0:
        # Used only for its side effect.
        self.fieldnames

    row = next(self.reader)
    self.line_num = self.reader.line_num

    d = dict(zip(self.fieldnames, row))
    lf = len(self.fieldnames)
    lr = len(row)
    if lf < lr:
        d[self.restkey] = row[lf:]
    elif lf > lr:
        for key in self.fieldnames[lr:]:
            d[key] = self.restval
    return d

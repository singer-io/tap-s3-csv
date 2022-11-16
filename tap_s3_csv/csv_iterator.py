import codecs
import csv

MAX_COL_LENGTH = 150


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

    reader.fieldnames, fieldname_pool = truncate_headers(reader.fieldnames)

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


# truncate headers that are longer than MAX_COL_LENGTH, then handle duplicates
def truncate_headers(column_names):
    fieldname_pool = set()
    final_fieldnames = []
    duplicatesExist = False

    for fieldname in column_names:
        if fieldname == '':
            final_fieldnames.append(fieldname)
            continue

        if len(fieldname) > MAX_COL_LENGTH:
            fieldname = fieldname[:MAX_COL_LENGTH]

        fieldname_lowercase = fieldname.casefold()
        if fieldname_lowercase not in fieldname_pool:
            fieldname_pool.add(fieldname_lowercase)
            final_fieldnames.append(fieldname)
        else:
            duplicatesExist = True
            break

    if not duplicatesExist:
        return final_fieldnames, fieldname_pool

    fieldname_pool = set()
    fieldname_first_occur = set()
    final_fieldnames = []

    # 4 chars are reserved for "_xxx" used to resolve duplicate names
    max_col_length = MAX_COL_LENGTH - 4

    for index, fieldname in enumerate(column_names):
        if fieldname == '':
            continue

        if len(fieldname) > max_col_length:
            fieldname = fieldname[:max_col_length]

        fieldname_lowercase = fieldname.casefold()
        if fieldname_lowercase not in fieldname_pool:
            fieldname_pool.add(fieldname_lowercase)
            fieldname_first_occur.add(index)

    for index, fieldname in enumerate(column_names):
        if fieldname == '':
            final_fieldnames.append(fieldname)
            continue

        if len(fieldname) > max_col_length:
            fieldname = fieldname[:max_col_length]

        if index in fieldname_first_occur:
            final_fieldnames.append(fieldname)
        else:
            fieldname_without_id, fieldname_lowercase_without_id, duplicate_id = split_fieldname_and_id(
                fieldname)

            duplicate_id += 1
            while f'{fieldname_lowercase_without_id}_{duplicate_id}' in fieldname_pool:
                duplicate_id += 1

            fieldname_pool.add(
                f'{fieldname_lowercase_without_id}_{duplicate_id}')
            final_fieldnames.append(
                f'{fieldname_without_id}_{duplicate_id}')

    return final_fieldnames, fieldname_pool


def split_fieldname_and_id(fieldname):
    fieldname_lowercase = fieldname.casefold()

    duplicate_id_index = fieldname_lowercase.rfind('_', -4)
    if duplicate_id_index != -1:
        duplicate_id_str = fieldname_lowercase[duplicate_id_index + 1:]
        if duplicate_id_str.isnumeric():
            duplicate_id = int(duplicate_id_str)
            fieldname_without_id = fieldname[:duplicate_id_index]
            fieldname_lowercase_without_id = fieldname_lowercase[:duplicate_id_index]

            return fieldname_without_id, fieldname_lowercase_without_id, duplicate_id

    return fieldname, fieldname_lowercase, 0


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

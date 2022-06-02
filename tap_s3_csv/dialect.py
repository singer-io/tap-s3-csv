import re
import random
import csv
import chardet
import clevercsv

from tap_s3_csv import s3

# We started using tap_s3_csv in 3.4 for both s3 and csv imports. Dialect detection
# is only run for csv imports
def detect_tables_dialect(config):
    # there is only one table in the array
    for table in config['tables']:
        # set is_csv_connector_import to True for imports from csv connector in Symon
        table['is_csv_connector_import'] = True
        # will return all matching files in s3 with given prefix and table name in config
        s3_files = s3.get_input_files_for_table(config, table)

        for s3_file in s3_files:
            detect_dialect(config, s3_file, table)


def detect_dialect(config, s3_file, table):
    config_delimiter = table.get('delimiter', '')
    config_quotechar = table.get('quotechar', '')
    config_encoding = table.get('encoding', '')

    detect_delimiter = config_delimiter == ''
    detect_quotechar = config_quotechar == ''
    detect_encoding = config_encoding == ''

    if not detect_encoding and not detect_delimiter and not detect_quotechar:
        return

    # clevercsv is good but slow - we cap it at 2000 rows, which is 1s of runtime on my machine
    MAX_DIALECT_LINES = 2000
    MAX_ENCODING_LINES = 10000
    MAX_LINES = MAX_ENCODING_LINES if detect_encoding else MAX_DIALECT_LINES

    # max bytes we want to cache in memory
    MAX_LINES_BYTES = 25 * 1024 ** 2

    # max bytes for each line read
    MAX_LINE_BYTES = 1024 ** 2

    # chardet is slow and rarely detects early. We limit the number of lines it is fed to keep performance acceptable.
    # The question is how many lines and how do we pick the most interesting lines?
    #
    # The answer below is the DETECT_CHARDET_LINE. The intuition is a line with all ASCII characters is unlikely to
    # change chardet's mind on anything. We took the HIGH_BYTE_DETECTOR and ESC_DETECTOR regexs from chardet and
    # combined them to filter for lines that are worth feeding into chardet. We can search through a large number of
    # lines for interesting ones quickly since chardet isn't involved. This rationale seems better than first x lines.
    #
    # We always take the first line since it may have BOM information and it's often the header. We think it's good
    # to take some of the beginning lines since if it fails to detect and the encoding is obvious to a person by
    # looking at the first few lines, it's especially offensive. People might be more understanding of a failed
    # detection if the key line is buried deep in the file.
    DETECT_CHARDET_LINE = re.compile(b'([\x80-\xFF]|(\033|~{))')
    MAX_CHARDET_LINES = 100
    FIRST_CHARDET_LINES = MAX_CHARDET_LINES / 10
    interesting = []
    interesting_map = {}

    lines = []
    lines_read = 0

    file_key = s3_file.get('key')
    file_handle = s3.get_file_handle(config, file_key)
    file_iter = file_handle.iter_lines()
    bytes_read = 0
    for i in range(MAX_LINES):
        try:
            line = next(file_iter)
            line_bytes = len(line)

            if line_bytes >= MAX_LINE_BYTES:
                raise Exception('Too many bytes in one line')
            
            lines_read += 1
            if bytes_read + line_bytes <= MAX_LINES_BYTES:
                lines.append(line)
                bytes_read += line_bytes

            if detect_encoding:
                if len(interesting) < MAX_CHARDET_LINES and (len(interesting) < FIRST_CHARDET_LINES or DETECT_CHARDET_LINE.search(line)):
                    interesting.append(i)

                    #  keep line that is not appended to lines array
                    if bytes_read + line_bytes > MAX_LINES_BYTES:
                        interesting_map[i] = line

        except StopIteration:
            break
    
    if detect_encoding:
        # finish preparing interesting lines - pad with non-interesting lines, keep original file order
        random.seed(0)
        remainder = min(MAX_CHARDET_LINES - len(interesting), lines_read - 1)

        for _ in range(remainder):
            i = random.randint(1, lines_read - 1)
            interesting.append(i)
        interesting.sort()

        # feed selected lines to universal detector
        detector = chardet.UniversalDetector()
        detector.MINIMUM_THRESHOLD = 0.70

        for i in interesting:
            # get line from cache
            if i < len(lines):
                line = lines[i]
            else:
                line = interesting_map[i]

            detector.feed(line)
            if detector.done:
                break
        
        # if detector had no results, default to utf-8
        detector_results = detector.close()
        encoding = detector_results.get('encoding', 'utf-8')
        confidence = detector_results.get('confidence', 1.0)

        # 1. ignore detector if confidence was low
        # 2. utf-8 is backwards compatible with ascii and supports more characters
        if confidence < .70 or encoding == 'ascii':
            encoding = 'utf-8'

        table['encoding'] = encoding
    
    # detect csv dialect
    if detect_delimiter or detect_quotechar:
        delimiter = ','
        quotechar = '"'

        MAX_DIALECT_CHARS = 100000

        # decode cached lines by detected/config encoding as sample
        decoded = []
        chars = 0
        for i, line in enumerate(lines):
            try:
                dline = line.decode(encoding)
            except UnicodeDecodeError as e:
                raise UnicodeDecodeError(
                    e.encoding, e.object, e.start, e.end, f'{e.reason} in line {i + 1}')

            # clevercsv seems to explode in memory to multiples of sample size
            # limit sample to a reasonable amount of characters to avoid memory issue
            if chars + len(dline) > MAX_DIALECT_CHARS:
                break

            decoded.append(dline)
            chars += len(dline)

            if i == MAX_DIALECT_LINES:
                break

        if len(decoded) > 0:
            try:
                # clevercsv is a drop-in replacement for python's csv module. The default csv module does a weak
                # job of detecting dialect. Clevercsv succeeded on client files csv failed to sniff.
                sample = '\n'.join(decoded)
                dialect = clevercsv.Sniffer().sniff(sample, [',', ';', '|', '^', '\t', ' '])
                delimiter = dialect.delimiter

                # we're currently only using clevercsv for dialect detection, csv can only handle 1 character
                # strings in quotechar. clevercsv produces multi-character strings at times, which csv will not
                # accept. For now, stick with the default in that case.
                if len(dialect.quotechar) == 1:
                    quotechar = dialect.quotechar
            except csv.Error:
                pass

        # if file is empty delimiter can be empty string.
        # setting it to the default value in this case so reader won't fail.
        if delimiter == '':
            delimiter = ','

        if detect_delimiter:
            table['delimiter'] = delimiter
        if detect_quotechar:
            table['quotechar'] = quotechar

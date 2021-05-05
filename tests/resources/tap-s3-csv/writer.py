import csv
import itertools
import sys
import random
import datetime

MAX_POS_DOUBLE = 1.7976931348623157E+308
MAX_POS_FLOAT = 3.402823466E+38
MAX_NEG_DOUBLE = -1.7976931348623157E+308
MAX_NEG_FLOAT = -3.402823466E+38
MIN_POS_DOUBLE = 2.2250738585072014E-308
MIN_POS_FLOAT = 1.175494351E-38
MIN_NEG_DOUBLE = -2.2250738585072014E-308
MIN_NEG_FLOAT = -1.175494351E-38

def test_string_data_by_columns():
    data = []
    for row in range(100):
        data.append([])
        for column in range(26):
            data[row].append(chr(ord("a") + column) + str(row))
    return list(zip(*data))

def test_string_type_max_length():

    data = list(zip(*[["a" * x, x] for x in itertools.chain(range(70), range(65507, 65537))]))
    data = [["string length test"] + list(data[0]), ["length of string"] + list(data[1])]
    return data


def test_unicode_characters(quoting: csv = csv.QUOTE_MINIMAL):
    with open('test_unicode_characters.csv', 'w') as file:
        write = csv.writer(file, quoting=quoting)
        write.writerow(["unicode character test", "character"])
        for char in range(sys.maxunicode):
            try:
                write.writerow([chr(char), char])
            except UnicodeEncodeError:
                pass


def test_bigint_valid_range():

    return [["bigint_signed"] +
            [random.randint(-2**63, 2**63-1) for _ in range(95)] +
            [-2**63, 2**63-1, -1, 0, 1]]


def test_unsigned_bigint_valid_range():

    return [["bigint_unsigned"] +
            [random.randint(2**63, 2**64) for _ in range(98)] +
            [-2**63, 2**63-1, -1, 0, 1]]


def test_beyond_bigint_valid_range():

    return [["larger_than_bigint"] +
            [random.randint(2**64+1, 2**65) for _ in range(49)] +
            [random.randint(-2**64, -2**63-1) for _ in range(49)] +
            [2**64+1, -2**63-1]]


def test_float_double_representable_range():

    # noinspection PyTypeChecker
    data = [["float at the edges", "double at the edges"]] + \
           [[random.uniform(MIN_POS_FLOAT, MAX_POS_FLOAT),
             random.uniform(MIN_POS_DOUBLE, MAX_POS_DOUBLE)] for _ in range(47)] + \
           [[random.uniform(MIN_NEG_FLOAT, MIN_NEG_FLOAT),
             random.uniform(MIN_NEG_DOUBLE, MAX_NEG_DOUBLE)] for _ in range(47)] + \
           [[MIN_POS_FLOAT, MIN_POS_DOUBLE], [MAX_POS_FLOAT, MAX_POS_DOUBLE],
            [MIN_NEG_FLOAT, MIN_NEG_DOUBLE], [MAX_NEG_FLOAT, MAX_NEG_DOUBLE],
            [0, 0], [0.0, 0.0]]

    return list(zip(*data))


def test_float_double_normal_range():
    return [["float with precision"] +
            [random.random() for _ in range(25)] +
            [random.random() * 10 ** 16 for _ in range(25)] +
            [-random.random() for _ in range(25)] +
            [random.random() * 10 ** 16 for _ in range(25)]]


def test_number_formats_commas():

    data = [["positive numbers with commas", "negative numbers with commas"]] + \
           [["{:,}".format(random.random() * 10 ** 10),
             "{:,}".format(-random.random() * 10 ** 10)] for _ in range(100)]

    return list(zip(*data))


def test_number_formats_plus_minus_signs():

    data = [["positive with various signs", "negative with signs"]] + \
           [["{:+f}".format(random.random() * 10 ** 10),
             "{:+f}".format(-random.random() * 10 ** 10)] for _ in range(34)] + \
           [["{: f}".format(random.random() * 10 ** 10),
             "{: f}".format(-random.random() * 10 ** 10)] for _ in range(34)] + \
           [["{:-f}".format(random.random() * 10 ** 10),
             "{:-f}".format(-random.random() * 10 ** 10)] for _ in range(32)]

    return list(zip(*data))


def test_number_formats_percentages():

    data = [["positive percentages", "negative percentages"]] + \
           [["{:%}".format(random.random()), "{:%}".format(-random.random())] for _ in range(100)]

    return list(zip(*data))


def test_not_real_numbers(quoting: csv = csv.QUOTE_MINIMAL):
    with open('test_not_real_numbers.csv', 'w') as file:
        write = csv.writer(file, quoting=quoting)
        write.writerow(["float", "double"])
        write.writerow([float("Inf"), -float("Inf")])
        write.writerow([float("nan"), -float("nan")])


def test_date_time_iso_format():

    data = []
    for row in range(100):
        timezone = datetime.timezone(datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
                                     "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        data += [[dt.date().isoformat(),
                 dt.time().isoformat(),
                 dt.replace(tzinfo=timezone.utc).isoformat(),
                 dt.isoformat()]]

    return_value = [["date", "time", "datetime without tz", "datetime with tz"]] + data
    return list(zip(*return_value))


def test_date_time_iso_format_space():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
                                     "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.replace(tzinfo=timezone.utc).isoformat(" "),
                 dt.isoformat(" ")]]

    return_value = [["datetime without tz space separator", "datetime with tz space separator"]] + rows
    return list(zip(*return_value))


def test_date_time_iso_format_to_hours():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(
            datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
            "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.time().isoformat(timespec="hours"),
                  dt.replace(tzinfo=timezone.utc).isoformat(timespec="hours"),
                  dt.isoformat(timespec="hours")]]

    return_value = [["time to hour", "datetime without tz to hour", "datetime with tz to hour"]] + rows
    return list(zip(*return_value))


def test_date_time_iso_format_to_minutes():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(
            datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
            "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.time().isoformat(timespec="minutes"),
                  dt.replace(tzinfo=timezone.utc).isoformat(timespec="minutes"),
                  dt.isoformat(timespec="minutes")]]

    return_value = [["time to minutes", "datetime without tz to minutes", "datetime with tz to minutes"]] + rows
    return list(zip(*return_value))


def test_date_time_iso_format_to_seconds():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(
            datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
            "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.time().isoformat(timespec="seconds"),
                  dt.replace(tzinfo=timezone.utc).isoformat(timespec="seconds"),
                  dt.isoformat(timespec="seconds")]]

    return_value = [["time to seconds", "datetime without tz to seconds", "datetime with tz to seconds"]] + rows
    return list(zip(*return_value))


def test_date_time_iso_format_to_milliseconds():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(
            datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
            "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.time().isoformat(timespec="milliseconds"),
                  dt.replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds"),
                  dt.isoformat(timespec="milliseconds")]]

    return_value = [["time to milliseconds",
                     "datetime without tz to milliseconds",
                     "datetime with tz to milliseconds"]] + rows

    return list(zip(*return_value))


def test_date_time_iso_format_to_microseconds():

    rows = []
    for row in range(100):
        timezone = datetime.timezone(
            datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
            "ABC")
        dt = datetime.datetime.now(tz=timezone) + datetime.timedelta(
            seconds=random.uniform(-60 * 60 * 24 * 365 * 100, 60 * 60 * 24 * 365 * 100))
        rows += [[dt.time().isoformat(timespec="microseconds"),
                  dt.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds"),
                  dt.isoformat(timespec="microseconds")]]

    return_value = [["time to microseconds",
                     "datetime without tz to microseconds",
                     "datetime with tz to microseconds"]] + rows

    return list(zip(*return_value))


def test_date_time_iso_format_min_max_values(quoting: csv = csv.QUOTE_MINIMAL):
    with open('test_date_time_iso_format_min_max_values.csv', 'w') as file:
        write = csv.writer(file, quoting=quoting)
        write.writerow(["date", "time", "datetime without tz", "datetime with tz"])
        timezone = datetime.timezone(datetime.timedelta(minutes=random.randint(0, 59), hours=random.randint(-23, 23)),
                                     "ABC")
        dt = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=timezone)
        write.writerow([dt.date().isoformat(),
                        dt.time().isoformat(),
                        dt.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds"),
                        dt.isoformat(timespec="microseconds")])

        dt = datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone)
        write.writerow([dt.date().isoformat(),
                        dt.time().isoformat(),
                        dt.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds"),
                        dt.isoformat(timespec="microseconds")])


# The JSON for this file does NOT contain regex pattern matching
def test_primary_key_unique_values_and_nullable_integers():

    data = [["key", "nullable integer", "non-nullable integer", "description"]] + \
           [[key,
             random.randint(1, 1000) if key != 11 else None,
             random.randint(1, 1000) if key != 12 else None,
             "passing row" if key != 12 else "failing row"] for key in range(1, 101)]

    return list(zip(*data))


# The JSON for this file DOES contain regex pattern matching
def test_primary_key_56_null_config(quoting: csv = csv.QUOTE_MINIMAL):
    with open('test_primary_key_56_null.csv', 'w') as file:
        write = csv.writer(file, quoting=quoting)
        write.writerow(["id", "value"])
        for pk in range(1, 100):
            if pk != 56:
                write.writerow([pk, random.randint(1, 1000)])
            else:
                write.writerow([None, random.randint(1, 1000)])


# The JSON for this file contains regex pattern matching that matches MULTIPLE files
def test_multiple_file_with_pk_part_a():
    data = [["key", "value"]] + \
           [[key, random.randint(1, 1000)] for key in range(1, 20)]

    return list(zip(*data))


# The JSON for this file contains regex pattern matching that matches MULTIPLE files
def test_multiple_file_with_pk_part_b():
    data = [["key", "value"]] + \
           [[key, random.randint(1, 1000)] for key in range(21, 40)]

    return list(zip(*data))


# The JSON for this file contains regex pattern matching that matches MULTIPLE files
def test_multiple_file_with_pk_part_c():
    data = [["key", "value"]] + \
           [[key, random.randint(1, 1000)] for key in range(61, 80)]

    return list(zip(*data))


# The JSON for this file contains regex pattern matching that matches MULTIPLE files
def test_header_order_multiple_file_with_pk_part_a():
    data = [["key", "value"]] + \
           [[key, random.randint(1, 1000)] for key in range(41, 60)]

    return list(zip(*data))


# The JSON for this file contains regex pattern matching that matches MULTIPLE files
def test_header_order_multiple_file_with_pk_part_b():
    data = [["value", "key"]] + \
           [[random.randint(1, 1000), key] for key in range(61, 80)]

    return list(zip(*data))


# The JSON for this file DOES contain regex pattern matching
def test_primary_key_56_duplicate_config(quoting: csv = csv.QUOTE_MINIMAL):
    with open('test_primary_key_56_duplicate.csv', 'w') as file:
        write = csv.writer(file, quoting=quoting)
        write.writerow(["id", "value"])
        for pk in range(1, 100):
            write.writerow([pk, random.randint(1, 1000)])
        write.writerow([56, random.randint(1, 1000)])


def write_csv_file(filename: str, rows_of_data: list, quoting: csv = csv.QUOTE_MINIMAL, rectangular: bool = True):

    if rectangular:
        len_first = len(rows_of_data[0]) if rows_of_data else None
        assert all(len(i) == len_first for i in rows_of_data)
    with open(filename, 'w') as file:
        write = csv.writer(file, quoting=quoting)
        for row in rows_of_data:
            write.writerow(row)


def main():

    columns = test_string_data_by_columns()
    rows = list(zip(*columns))
    rows[0] = rows[0][0:-5]  # delete the last 5 headers
    write_csv_file("header_shorter.csv", rows, rectangular=False)

    rows = list(zip(*columns))
    rows[0] = rows[0] + ("aa0", "ab0", "ac0", "ad0", "ae0")
    write_csv_file("header_longer.csv", rows, rectangular=False)

    rows = list(zip(*columns))
    rows[1] = rows[1] + ("aa1", "ab1", "ac1", "ad1", "ae1")
    rows[2] = rows[2][0:-5]
    write_csv_file("rows_longer_and_shorter.csv", rows, rectangular=False)

    # remove header from column
    integer_column = test_bigint_valid_range()[0][1:]
    number_column = test_float_double_normal_range()[0][1:]
    date_time_column = list(test_date_time_iso_format()[2][1:])
    string_column = list(test_string_data_by_columns()[0])

    columns = [["integer_to_number"] + integer_column + number_column]
    columns += [["number_to_integer"] + number_column + integer_column]
    columns += [["number_to_string"] + number_column + string_column]
    columns += [["string_to_integer"] + string_column + integer_column]
    columns += [["string_to_date_time"] + string_column + date_time_column]
    columns += [["date_time_to_integer"] + date_time_column + integer_column]
    columns += [["date_time_to_integer_override"] + date_time_column + integer_column]
    write_csv_file("test_switching_data_types.csv", list(zip(*columns)))

    columns = test_string_type_max_length()
    columns += test_bigint_valid_range()
    columns += test_unsigned_bigint_valid_range()
    columns += test_beyond_bigint_valid_range()
    columns += test_float_double_representable_range()
    columns += test_float_double_normal_range()
    columns += test_number_formats_commas()
    columns += test_number_formats_plus_minus_signs()
    columns += test_number_formats_percentages()
    columns += test_date_time_iso_format()
    columns += test_date_time_iso_format_space()
    columns += test_date_time_iso_format_to_hours()
    columns += test_date_time_iso_format_to_minutes()
    columns += test_date_time_iso_format_to_seconds()
    columns += test_date_time_iso_format_to_milliseconds()
    columns += test_date_time_iso_format_to_microseconds()
    write_csv_file("test_data_types_all_quoted.csv", list(zip(*columns)), quoting=csv.QUOTE_ALL)
    write_csv_file("test_data_types.csv", list(zip(*columns)))

    columns = test_primary_key_unique_values_and_nullable_integers()
    write_csv_file("primary_key_unique_values_and_nullable_integers.csv", list(zip(*columns)))

    write_csv_file("test_multiple_file_with_pk_part_a.csv", list(zip(*test_multiple_file_with_pk_part_a())))
    write_csv_file("test_multiple_file_with_pk_part_b.csv", list(zip(*test_multiple_file_with_pk_part_b())))
    write_csv_file("test_multiple_file_with_pk_part_c.csv", list(zip(*test_multiple_file_with_pk_part_c())))

    write_csv_file(
        "test_header_order_multiple_file_with_pk_part_a.csv",
        list(zip(*test_header_order_multiple_file_with_pk_part_a())))
    write_csv_file(
        "test_header_order_multiple_file_with_pk_part_b.csv",
        list(zip(*test_header_order_multiple_file_with_pk_part_b())))

    test_unicode_characters()
    test_not_real_numbers()
    test_date_time_iso_format_min_max_values()

    test_primary_key_56_null_config()
    test_primary_key_56_duplicate_config()

    # test_bigint_valid_range(csv.QUOTE_ALL)


if __name__ == "__main__":
    main()

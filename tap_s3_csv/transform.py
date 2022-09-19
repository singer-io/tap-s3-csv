from dataclasses import replace
import datetime
import decimal
import logging
import re

import singer.metadata
from singer.logger import get_logger
from singer.utils import (strftime, strptime_to_utc)

LOGGER = get_logger()

NO_INTEGER_DATETIME_PARSING = 'no-integer-datetime-parsing'
UNIX_SECONDS_INTEGER_DATETIME_PARSING = 'unix-seconds-integer-datetime-parsing'
UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING = 'unix-milliseconds-integer-datetime-parsing'

VALID_DATETIME_FORMATS = [
    NO_INTEGER_DATETIME_PARSING,
    UNIX_SECONDS_INTEGER_DATETIME_PARSING,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
]

def string_to_datetime(value):
    try:
        return strftime(strptime_to_utc(value))
    except Exception as ex:
        LOGGER.warning('%s, (%s)', ex, value)
        return None


def unix_milliseconds_to_datetime(value):
    return strftime(datetime.datetime.fromtimestamp(float(value) / 1000.0, datetime.timezone.utc))


def unix_seconds_to_datetime(value):
    return strftime(datetime.datetime.fromtimestamp(int(value), datetime.timezone.utc))


def breadcrumb_path(breadcrumb):
    """
    Transform breadcrumb into familiar object dot-notation
    """
    name = '.'.join(breadcrumb)
    name = name.replace('properties.', '')
    name = name.replace('.items', '[]')
    return name


class SchemaMismatch(Exception):
    def __init__(self, errors):
        if not errors:
            msg = 'An error occured during transform that was not a schema mismatch'

        else:
            estrs = [e.tostr() for e in errors]
            msg = 'Errors during transform\n\t{}'.format('\n\t'.join(estrs))
            msg += '\n\n\nErrors during transform: [{}]'.format(
                ', '.join(estrs))

        super().__init__(msg)


class SchemaKey:
    ref = '$ref'
    items = 'items'
    properties = 'properties'
    pattern_properties = 'patternProperties'
    any_of = 'anyOf'


class Error:
    def __init__(self, path, data, schema=None, logging_level=logging.INFO):
        self.path = path
        self.data = data
        self.schema = schema
        self.logging_level = logging_level

    def tostr(self):
        path = '.'.join(map(str, self.path))
        if self.schema:
            if self.logging_level >= logging.INFO:
                msg = 'data does not match {}'.format(self.schema)
            else:
                msg = 'does not match {}'.format(self.schema)
        else:
            msg = 'not in schema'

        if self.logging_level >= logging.INFO:
            output = '{}: {}'.format(path, msg)
        else:
            output = '{}: {} {}'.format(path, self.data, msg)
        return output


class Transformer:
    def __init__(self, source_type_map, integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING, pre_hook=None):
        self.integer_datetime_fmt = integer_datetime_fmt
        self.pre_hook = pre_hook
        self.removed = set()
        self.filtered = set()
        self.errors = []
        self.source_type_map = source_type_map

    def log_warning(self):
        if self.filtered:
            LOGGER.debug('Filtered %s paths during transforms '
                         'as they were unsupported or not selected:\n\t%s',
                         len(self.filtered),
                         '\n\t'.join(sorted(self.filtered)))
            # Output list format to parse for reporting
            LOGGER.debug('Filtered paths list: %s',
                         sorted(self.filtered))

        if self.removed:
            LOGGER.debug('Removed %s paths during transforms:\n\t%s',
                         len(self.removed),
                         '\n\t'.join(sorted(self.removed)))
            # Output list format to parse for reporting
            LOGGER.debug('Removed paths list: %s', sorted(self.removed))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.log_warning()

    def cleanup(self):
        self.log_warning()
        self.removed.clear()
        self.filtered.clear()
        self.errors.clear()

    def filter_data_by_metadata(self, data, auto_fields, filter_fields, parent=()):
        if isinstance(data, dict) and filter_fields:
            for field_name in list(data.keys()):
                breadcrumb = parent + ('properties', field_name)
                if breadcrumb in auto_fields:
                    continue

                if breadcrumb in filter_fields:
                    data.pop(field_name, None)
                    # Track that a field was filtered because the customer
                    # didn't select it or the tap declared it as unsupported.
                    self.filtered.add(breadcrumb_path(breadcrumb))
                else:
                    data[field_name] = self.filter_data_by_metadata(
                        data[field_name], auto_fields, filter_fields, breadcrumb)

        if isinstance(data, list) and filter_fields:
            breadcrumb = parent + ('items',)
            data = [self.filter_data_by_metadata(
                d, auto_fields, filter_fields, breadcrumb) for d in data]

        return data

    def transform(self, data, schema, auto_fields, filter_fields):
        data = self.filter_data_by_metadata(data, auto_fields, filter_fields)

        success, transformed_data = self.transform_recur(data, schema, [])
        if not success:
            raise SchemaMismatch(self.errors)

        return transformed_data

    def transform_recur(self, data, schema, path, source_type=None):
        if 'anyOf' in schema:
            return self._transform_anyof(data, schema, path)

        if 'type' not in schema:
            # indicates no typing information so don't bother transforming it
            return True, data

        types = schema["type"]
        for typ in types:
            success, transformed_data = self._transform(
                data, typ, schema, path, source_type)
            if success:
                return success, transformed_data
        else:  # pylint: disable=useless-else-on-loop
            # exhaused all types and didn't return, so we failed :-(
            self.errors.append(
                Error(path, data, schema, logging_level=LOGGER.level))
            return False, None

    def transform_schema_recur(self, schema):
        if "type" not in schema:
            return
        types = schema["type"]
        if not isinstance(types, list):
            types = [types]
            schema["type"] = types

        # modify schema to put null as the last type to check for
        # e.g. ['null', 'integer'] -> ['integer', 'null']
        if "null" in types and types[-1] != "null":
            types.remove("null")
            types.append("null")

        for typ in types:
            if typ == "object" and "properties" in schema:
                self._transform_schema_nested(schema.get("properties", {}))

            elif typ == "array":
                self.transform_schema_recur(schema["items"])

    def _transform_schema_nested(self, schema):
        if schema == {}:
            return
        for sub_schema in schema:
            self.transform_schema_recur(schema[sub_schema])

    def _transform_anyof(self, data, schema, path):
        subschemas = schema['anyOf']
        for subschema in subschemas:
            success, transformed_data = self.transform_recur(
                data, subschema, path)
            if success:
                return success, transformed_data
        else:  # pylint: disable=useless-else-on-loop
            # exhaused all schemas and didn't return, so we failed :-(
            self.errors.append(
                Error(path, data, schema, logging_level=LOGGER.level))
            return False, None

    def _transform_object(self, data, schema, path, pattern_properties):
        # We do not necessarily have a dict to transform here. The schema's
        # type could contain multiple possible values. Eg:
        #     ['null', 'object', 'string']
        if not isinstance(data, dict):
            return False, data

        # Don't touch an empty schema
        if schema == {} and not pattern_properties:
            return True, data

        result = {}
        successes = []
        for key, value in data.items():
            # patternProperties are a map of {'pattern': { schema...}}
            pattern_schemas = [schema for pattern, schema
                               in (pattern_properties or {}).items()
                               if re.match(pattern, key)]
            if key in schema or pattern_schemas:
                sub_schema = schema.get(key, {'anyOf': pattern_schemas})
                success, subdata = self.transform_recur(
                    value, sub_schema, path + [key], self.source_type_map.get(key))
                successes.append(success)
                result[key] = subdata
            else:
                # track that field has been removed because it wasn't
                # found in the schema. This likely indicates some problem
                # with discovery but rather than failing the run because
                # new data was added we'd rather continue the sync and
                # allow customers to indicate that they want the new data.
                self.removed.add('.'.join(map(str, path + [key])))

        return all(successes), result

    def _transform_array(self, data, schema, path):
        # We do not necessarily have a list to transform here. The schema's
        # type could contain multiple possible values. Eg:
        #     ['null', 'array', 'integer']
        if not isinstance(data, list):
            return False, data
        result = []
        successes = []
        for i, row in enumerate(data):
            success, subdata = self.transform_recur(row, schema, path + [i])
            successes.append(success)
            result.append(subdata)

        return all(successes), result

    def _transform_datetime(self, value):
        if value is None or value == '':
            return None  # Short circuit in the case of null or empty string

        if self.integer_datetime_fmt not in VALID_DATETIME_FORMATS:
            raise Exception('Invalid integer datetime parsing option')

        if self.integer_datetime_fmt == NO_INTEGER_DATETIME_PARSING:
            return string_to_datetime(value)
        else:
            try:
                if self.integer_datetime_fmt == UNIX_SECONDS_INTEGER_DATETIME_PARSING:
                    return unix_seconds_to_datetime(value)
                else:
                    return unix_milliseconds_to_datetime(value)
            except:
                return string_to_datetime(value)

    def _get_transformvalue_by_type(self, data, type):
        if type == 'string':
            if data is not None:
                try:
                    return True, str(data)
                except:
                    return False, None
            else:
                return False, None

        elif type == 'integer':
            if isinstance(data, str):
                data = data.replace(",", "")

            try:
                return True, int(data)
            except:
                return False, None

        elif type == 'number':
            if isinstance(data, str):
                data = data.replace(",", "")

            try:
                return True, float(data)
            except:
                return False, None

        elif type == 'boolean':
            if isinstance(data, str) and data.lower() == "false":
                return True, False

            try:
                return True, bool(data)
            except:
                return False, None

        else:
            return False, None

    def _transform(self, data, typ, schema, path, source_type=None):
        if source_type:
            return self._get_transformvalue_by_type(data, source_type)

        if self.pre_hook:
            data = self.pre_hook(data, typ, schema)

        if typ == 'null':
            if data is None or data == '' or data == '<null>':
                return True, None
            else:
                return False, None

        elif schema.get('format') == 'date-time':
            data = self._transform_datetime(data)
            if data is None:
                return False, None

            return True, data
        elif schema.get('format') == 'singer.decimal':
            if data is None:
                return False, None

            if isinstance(data, (str, float, int)):
                try:
                    return True, str(decimal.Decimal(str(data)))
                except:
                    return False, None
            elif isinstance(data, decimal.Decimal):
                try:
                    if data.is_snan():
                        return True, 'NaN'
                    else:
                        return True, str(data)
                except:
                    return False, None

            return False, None
        elif typ == 'object':
            # Objects do not necessarily specify properties
            return self._transform_object(data,
                                          schema.get('properties', {}),
                                          path,
                                          schema.get(SchemaKey.pattern_properties))

        elif typ == 'array':
            return self._transform_array(data, schema['items'], path)

        else:
            return self._get_transformvalue_by_type(data, typ)


def resolve_filter_fields(metadata=None):
    autos = set()
    filters = set()
    source_type_map = dict()

    if metadata:
        for breadcrumb in sorted(metadata, key=len):
            if breadcrumb == ():
                continue

            # check if any ancestor breadcrumbs are automatic
            ancestor_auto = False
            for length in range(len(breadcrumb) - 2, 0, -2):
                if (breadcrumb[:length]) in autos:
                    ancestor_auto = True
                    break

            inclusion = singer.metadata.get(metadata, breadcrumb, 'inclusion')

            if ancestor_auto or inclusion == 'automatic':
                autos.add(breadcrumb)
                continue

            selected = singer.metadata.get(metadata, breadcrumb, 'selected')
            if (selected is False) or (inclusion == 'unsupported'):
                filters.add(breadcrumb)

            source_type = singer.metadata.get(
                metadata, breadcrumb, 'source_type')
            if source_type:
                source_type_map[breadcrumb_path(breadcrumb)] = source_type

    return frozenset(autos), frozenset(filters), source_type_map

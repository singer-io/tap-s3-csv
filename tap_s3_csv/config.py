from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema([{
    Required('table_name'): str,
    Required('search_pattern'): str,
    Required('key_properties'): [str],
    Optional('search_prefix'): str,
    Optional('date_overrides'): [str],
    Optional('delimiter'): str,
    Optional('escape_char'): str,
    Optional('recursive_search'): bool,
    Optional('quotechar'): str,
    Optional('skip_header_row'): int,
    Optional('skip_footer_row'): int,
    Optional('has_header'): bool,
    Optional('row_limit'): int,
}])

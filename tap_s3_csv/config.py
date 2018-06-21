from voluptuous import Schema, Required, Any, Optional

CONFIG_CONTRACT = Schema([{
    Required('name'): str,
    Required('pattern'): str,
    Required('key_properties'): [str],
    Optional('search_prefix'): str,
    Optional('schema_overrides'): {
        str: {
            Required('type'): Any(str, [str]),
            Required('_conversion_type'): Any('string',
                                              'integer',
                                              'number',
                                              'date-time')
        }
    }
}])

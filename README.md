# tap-s3-csv

This is a [Singer](https://singer.io) tap that reads data from files located inside a given S3 bucket and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## How to use it

`tap-s3-csv` works together with any other [Singer Target](https://singer.io) to move data from s3 to any target destination.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
 python3 -m venv ~/.virtualenvs/tap-s3-csv
 source ~/.virtualenvs/tap-s3-csv/bin/activate
 pip install -U pip setuptools
 pip install -e '.[dev]'
```

### Configuration

Here is an example of basic config, and a bit of a run down on each of the properties:

```
{
    "bucket": "my-bucket",
    "tables": "[
        {
            "search_prefix": "exports",
            "search_pattern": "my_table\/.*\.csv",
            "table_name": "my_table",
            "key_properties": "id",
            "date_overrides": "created_at",
            "delimiter": ","
            "escape_char": "\"
        }
    ]"
}
```

- **bucket**: The name of the bucket to search for files under.
- **tables**: Used to search for files, and emit records as "tables" from those files. Will be used by a [`voluptuous`](https://github.com/alecthomas/voluptuous)-based configuration checker.

The `table` field consists of one or more objects that describe how to find files and emit records. A more detailed example below:

```
[
    {
        "search_prefix": "exports"
        "search_pattern": "my_table\/.*\.csv",
        "table_name": "my_table",
        "key_properties": "id",
        "date_overrides": "created_at",
        "delimiter": ",",
        "escape_char": "\"
    },
    ...
]
```

- **search_prefix**: This is a prefix to apply after the bucket, but before the file search pattern, to allow you to find files in "directories" below the bucket.
- **search_pattern**: This is an escaped regular expression that the tap will use to find files in the bucket + prefix. It's a bit strange, since this is an escaped string inside of an escaped string, any backslashes in the RegEx will need to be double-escaped.
- **table_name**: This value is a string of your choosing, and will be used to name the stream that records are emitted under for files matching content.
- **key_properties**: These are the "primary keys" of the CSV files, to be used by the target for deduplication and primary key definitions downstream in the destination.
- **date_overrides**: Specifies field names in the files that are supposed to be parsed as a datetime. The tap doesn't attempt to automatically determine if a field is a datetime, so this will make it explicit in the discovered schema.
- **delimiter**: This allows you to specify a custom delimiter, such as `\t` or `|`, if that applies to your files.
- **escape_char**: This allows you to specify a single escape character (default is `\`) if that applies to your files.

A sample configuration is available inside [config.sample.json](config.sample.json)

### Configuration when your source file exists in an external AWS account

```
{
    "bucket": "bucket-name",
    "account_id": "111222333444",
    "role_name": "role name in external AWS account giving your AWS account permission to access their S3 bucket",
    "external_id": "external id defined in role in external AWS account giving your AWS account permission to access their S3 bucket",
    "tables": "[
        {
            "search_prefix": "exports",
            "search_pattern": "my_table\/.*\.csv",
            "table_name": "my_table",
            "key_properties": "id",
            "date_overrides": "created_at",
            "delimiter": ","
            "escape_char": "\",
            "recursive_search": false
        }
    ]"
}
```

- **account_id**: The AWS account id of the external AWS account you are trying to get the file from
- **role_name**: The name of the role set up in the external AWS account to provide you access to their S3 bucket
- **external_id**: The external_id defined in the role to help authorize your AWS account when connecting to the external AWS account
- **recursive_search**: true/false/undefined

A note about `recursive_search` property: By default (with `recursive_search` undefined or set to true), the tap will select files in your S3 bucket whose file names match the `search_pattern` regex in the folder you specify with `search_prefix`, and any subfolders within the folder. If multiple files are found in the folder structure that match the `search_pattern`, the content of all of the files will be combined. For discovery, this means all columns from all files will be present in the catalog that gets produced, and for import, it means all columns and all rows from all files will be present in the resulting output (for files that don’t include columns that are present in other selected files, the corresponding cells for those rows will just be blank). This behaviour could potentially be beneficial if you have multiple files with the same schema, and you would like the tap to just combine the rows. However, it could also lead to undesired results if multiple files within the same folder structure just happen to match the same `search_pattern`, but aren’t intended to be related. To limit the search to exactly folder specified with `search_prefix`, set `recursive_search` to false.

---

Copyright &copy; 2018 Stitch

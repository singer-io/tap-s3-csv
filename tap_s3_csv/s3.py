import functools
import re

import backoff
import boto3
from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from botocore.session import Session
from botocore.config import Config
from botocore.paginate import PageIterator

import singer

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"

LOGGER = singer.get_logger()


def retry_pattern(fnc):
    @backoff.on_exception(backoff.expo,
                          (ConnectTimeoutError, ReadTimeoutError),
                          max_tries=5,
                          on_backoff=log_backoff_attempt,
                          factor=2)
    @backoff.on_exception(backoff.expo,
                          ClientError,
                          max_tries=5,
                          on_backoff=log_backoff_attempt,
                          factor=10)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


# Added decorator over functions of botocore SDK as functions from SDK returns generator and
# tap is yielding data from that function so backoff is not working over tap function(list_files_in_bucket()).
PageIterator._make_request = retry_pattern(PageIterator._make_request)


class AssumeRoleProvider():
    METHOD = 'assume-role'

    def __init__(self, fetcher):
        self._fetcher = fetcher

    def load(self):
        return DeferredRefreshableCredentials(
            self._fetcher.fetch_credentials,
            self.METHOD
        )


class S3Client:

    def __init__(self, config):
        self.config = config

    def get_request_timeout(self):
        """Function to return timeout value"""
        # Get `request_timeout` value from config.
        config_request_timeout = self.config.get('request_timeout')

        # if config request_timeout is other than 0,"0" or "" then use request_timeout
        if config_request_timeout and float(config_request_timeout):
            request_timeout = float(config_request_timeout)
        else:
            # If the value is 0, "0", "" or not passed then it set the default to 300 seconds.
            request_timeout = 300
        return request_timeout

    @retry_pattern
    def setup_aws_client(self):
        """Function to setup AWS client as per the config credentials"""
        role_arn = "arn:aws:iam::{}:role/{}".format(self.config['account_id'].replace('-', ''),
                                                    self.config['role_name'])
        session = Session()
        fetcher = AssumeRoleCredentialFetcher(
            session.create_client,
            session.get_credentials(),
            role_arn,
            extra_args={
                'DurationSeconds': 3600,
                'RoleSessionName': 'TapS3CSV',
                'ExternalId': self.config['external_id']
            },
            cache=JSONFileCache()
        )

        refreshable_session = Session()
        refreshable_session.register_component(
            'credential_provider',
            CredentialResolver([AssumeRoleProvider(fetcher)])
        )

        LOGGER.info("Attempting to assume_role on RoleArn: %s", role_arn)
        boto3.setup_default_session(botocore_session=refreshable_session)

    @retry_pattern
    def get_file_handle(self, s3_path):
        """Function to return file object as per the given S3 Path"""
        bucket = self.config['bucket']
        # Set connect and read timeout for resource
        timeout = self.get_request_timeout()
        client_config = Config(connect_timeout=timeout, read_timeout=timeout)
        s3_client = boto3.resource('s3', config=client_config)

        s3_bucket = s3_client.Bucket(bucket)
        s3_object = s3_bucket.Object(s3_path['filepath'])
        file_handle = s3_object.get()['Body']
        return file_handle._raw_stream if hasattr(file_handle, "_raw_stream") else file_handle

    @retry_pattern
    def list_files_in_bucket(self, search_prefix=None):
        """Function to list the files on the S3 Bucket"""
        # Set connect and read timeout for resource
        timeout = self.get_request_timeout()
        client_config = Config(connect_timeout=timeout,  read_timeout=timeout)
        s3_client = boto3.client('s3', config=client_config)

        s3_object_count = 0

        max_results = 1000
        bucket = self.config['bucket']
        args = {
            'Bucket': bucket,
            'MaxKeys': max_results,
        }

        if search_prefix is not None:
            args['Prefix'] = search_prefix

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = 0
        for page in paginator.paginate(**args):
            pages += 1
            LOGGER.debug("On page %s", pages)
            s3_object_count += len(page['Contents'])
            yield from page['Contents']

        if s3_object_count > 0:
            LOGGER.info("Found %s files.", s3_object_count)
        else:
            LOGGER.warning(
                'Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)

    def get_files(self, table_spec, modified_since=None):
        """Function to return matching files as per the provided search pattern"""

        search_prefix = table_spec['search_prefix']
        search_pattern = table_spec['search_pattern']

        try:
            matcher = re.compile(search_pattern)
        except re.error as e:
            raise ValueError(
                ("search_pattern for table `{}` is not a valid regular "
                 "expression. See "
                 "https://docs.python.org/3.5/library/re.html#regular-expression-syntax").format(table_spec['table_name']),
                search_pattern) from e

        LOGGER.info('Checking bucket "%s" for keys matching "%s"',
                     self.config['bucket'], search_pattern)

        matched_files_count = 0
        unmatched_files_count = 0
        max_files_before_log = 30000

        for s3_object in self.list_files_in_bucket(search_prefix):
            key = s3_object['Key']
            last_modified = s3_object['LastModified']

            if matcher.search(key):
                matched_files_count += 1
                if modified_since is None or modified_since < last_modified:
                    LOGGER.info(
                        'Will download key "%s" as it was last modified %s', key, last_modified)
                    yield {'filepath': key, 'last_modified': last_modified}
            else:
                unmatched_files_count += 1

            if (unmatched_files_count + matched_files_count) % max_files_before_log == 0:
                # Are we skipping greater than 50% of the files?
                if (unmatched_files_count / (matched_files_count + unmatched_files_count)) > 0.5:
                    LOGGER.warn(("Found %s matching files and %s non-matching files. "
                                  "You should consider adding a `search_prefix` to the config "
                                  "or removing non-matching files from the bucket."),
                                 matched_files_count, unmatched_files_count)
                else:
                    LOGGER.info("Found %s matching files and %s non-matching files",
                                 matched_files_count, unmatched_files_count)

        # Raise an exception if no matching files are found
        if matched_files_count == 0:
            raise Exception("No files found matching pattern {}".format(search_pattern))

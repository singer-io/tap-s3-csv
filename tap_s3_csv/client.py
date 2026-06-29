from botocore.exceptions import ClientError

from tap_s3_csv import s3
from tap_s3_csv.exceptions import S3CsvForbiddenError


def _is_forbidden_client_error(error):
    if not isinstance(error, ClientError):
        return False
    code = error.response.get("Error", {}).get("Code")
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in ("AccessDenied", "UnauthorizedOperation") or status_code == 403


class S3CsvClient:
    def __init__(self, config):
        self.config = config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def make_request(self, method, _url=None, params=None, _headers=None, _body=None):
        if method != "LIST":
            raise NotImplementedError("S3CsvClient supports only LIST method for access checks")

        search_prefix = (params or {}).get("search_prefix")
        try:
            next(s3.list_files_in_bucket(self.config, search_prefix), None)
        except ClientError as error:
            if _is_forbidden_client_error(error):
                raise S3CsvForbiddenError(
                    "HTTP-error-code: 403, Error: The credentials do not have read access to this S3 resource.",
                    error.response,
                ) from error
            raise

        return None

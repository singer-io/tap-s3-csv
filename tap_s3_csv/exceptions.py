class S3CsvForbiddenError(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response

class SymonException(Exception):
    def __init__(self, message, code, details=None):
        super().__init__(message)
        self.code = code
        self.details = details

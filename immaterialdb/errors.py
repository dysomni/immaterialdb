class FieldMisconfigurationError(Exception):
    pass


class ModelMisconfigurationError(Exception):
    pass


class QueryNotSupportedError(Exception):
    pass


class RecordNotUniqueError(Exception):
    pass


class LockNotAcquiredError(Exception):
    pass


class ConcurrentRecordUpdateError(LockNotAcquiredError):
    pass

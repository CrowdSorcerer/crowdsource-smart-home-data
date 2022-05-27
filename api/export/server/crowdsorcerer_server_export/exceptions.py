from http.client import BAD_REQUEST, NO_CONTENT
from crowdsorcerer_server_export.df2CKAN import EXPORT_FORMATS



class BadDateFormat(RuntimeError):
    """Supplied date parameter has an incorrect format (should be ISO 8601, at UTC+0)."""
    
    def __init__(self, parameter: str=None):
        self.parameter = parameter

    def __str__(self):
        if self.parameter:
            return f"Supplied date parameter '{self.parameter}' has an incorrect format (should be ISO 8601, at UTC+0)."
        return "Supplied date parameter has an incorrect format (should be ISO 8601, at UTC+0)."

class UnsupportedExportationFormat(RuntimeError):
    """Specified exportation format is not supported."""

    def __init__(self, format_: str=None):
        self.format_ = format_

    def __str__(self):
        supported_str = str(list(EXPORT_FORMATS.keys()))[1:-1]
        if self.format_:
            return f"Specified exportation format '{self.format_}' is not supported. Supported formats are: {supported_str}."
        return f"Specified exportation format is not supported. Supported formats are: {supported_str}."

class EmptyDataset(RuntimeError):
    """The provided query filters produce a dataset without data columns or rows."""

    def __str__(self):
        return "The provided query filters produce a dataset without data columns or rows."



BAD_DATE_FORMAT = {
    'error_code': BadDateFormat,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

UNSUPPORTED_EXPORTATION_FORMAT = {
    'error_code': UnsupportedExportationFormat,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

EMTPY_DATASET = {
    'error_code': EmptyDataset,
    'function': lambda error: ({
            'detail': str(error),
            'status': NO_CONTENT,
            'title': 'No Content'
        }, NO_CONTENT)
}

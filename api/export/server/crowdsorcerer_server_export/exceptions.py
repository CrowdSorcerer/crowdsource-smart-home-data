from http.client import BAD_REQUEST

class BadDateFormat(RuntimeError):
    """Supplied date parameter has an incorrect format (should be ISO 8601, at UTC+0)."""
    
    def __init__(self, parameter: str=None):
        self.parameter = parameter

    def __str__(self):
        if self.parameter:
            return f"Supplied date parameter '{self.parameter}' has an incorrect format (should be ISO 8601, at UTC+0)."
        return "Supplied date parameter has an incorrect format (should be ISO 8601, at UTC+0)."



BAD_DATE_FORMAT = {
    'error_code': BadDateFormat,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

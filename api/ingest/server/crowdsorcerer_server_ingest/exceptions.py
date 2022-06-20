from http.client import BAD_REQUEST

class MalformedUUID(RuntimeError):
    """Supplied UUID in the header field 'Home-UUID' was not properly formatted."""
    
    def __str__(self):
        return "Supplied UUID in the header field 'Home-UUID' was not properly formatted."

class BadIngestDecoding(RuntimeError):
    """Data uploaded is not properly decoded (JSON -> UTF-8 encode -> zlib)."""

    def __str__(self):
        return "Data uploaded is not properly decoded (JSON -> UTF-8 encode -> zlib)."

class BadJSONStructure(RuntimeError):
    """The decoded JSON data is not a JSON object."""

    def __str__(self):
        return "The decoded JSON data is not a JSON object."

class InvalidJSONKey(RuntimeError):
    """The provided JSON object has a key with an invalid name (starts with \"_hoodie\")."""

    def __str__(self):
        return "The provided JSON object has a key with an invalid name (starts with \"_hoodie\")."



MALFORMED_UUID = {
    'error_code': MalformedUUID,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

BAD_INGEST_DECODING = {
    'error_code': BadIngestDecoding,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

BAD_JSON_STRUCTURE = {
    'error_code': BadJSONStructure,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

INVALID_JSON_KEY = {
    'error_code': InvalidJSONKey,
    'function': lambda error: ({
            'detail': str(error),
            'status': BAD_REQUEST,
            'title': 'Bad Request'
        }, BAD_REQUEST)
}

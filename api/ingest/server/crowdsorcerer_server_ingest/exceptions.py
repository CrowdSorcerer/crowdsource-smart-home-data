from http.client import BAD_REQUEST

class MalformedUUID(RuntimeError):
    """Supplied UUID in the header field 'Home-UUID' was not properly formatted."""
    
    def __str__(self):
        return "Supplied UUID in the header field 'Home-UUID' was not properly formatted."

class BadIngestDecoding(RuntimeError):
    """Data uploaded is not properly decoded (JSON -> UTF-8 encode -> zlib)."""

    def __str__(self):
        return "Data uploaded is not properly decoded (JSON -> UTF-8 encode -> zlib)."



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

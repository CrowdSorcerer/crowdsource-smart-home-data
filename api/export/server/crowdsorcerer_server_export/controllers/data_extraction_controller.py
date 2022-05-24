import connexion
import six

from .implementation import extract


def data_extraction(format_, date_from=None, date_to=None, types=None):  # noqa: E501
    """Extract data from the data lake into a CKAN compliant format, zipped.

     # noqa: E501

    :param format: The case insensitive string representing the dataset&#x27;s output format
    :type format: str
    :param date_from: Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
    :type date_from: str
    :param date_to: Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
    :type date_to: str
    :param types: Only data from these types of producer will be extracted (e.g. sensor)
    :type types: List[str]

    :rtype: None
    """
    
    return extract(format_, date_from, date_to, types)

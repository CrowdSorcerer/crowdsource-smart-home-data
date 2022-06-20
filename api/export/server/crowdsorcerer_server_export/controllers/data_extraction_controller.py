import connexion
import six

from crowdsorcerer_server_export import util
from crowdsorcerer_server_export.controllers.implementation import extract
from crowdsorcerer_server_export.df2CKAN import EXPORT_FORMATS


def available_formats():  # noqa: E501
    """List of the available extraction formats

     # noqa: E501


    :rtype: str
    """
    
    return str(list(EXPORT_FORMATS.keys()))[1:-1].replace("'", "")


def data_extraction(formats, date_from=None, date_to=None, types=None, units=None):  # noqa: E501
    """Extract data from the data lake into a CKAN compliant format, zipped.

     # noqa: E501

    :param formats: The case insensitive strings representing the dataset&#x27;s output formats
    :type formats: List[str]
    :param date_from: Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive
    :type date_from: str
    :param date_to: Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive
    :type date_to: str
    :param types: Only data from these types of producer will be extracted (e.g. sensor)
    :type types: List[str]
    :param units: Only data which is represented in the specified units of measurement will be extracted (e.g. GHz)
    :type units: List[str]

    :rtype: None
    """

    return extract(formats, date_from, date_to, types, units)

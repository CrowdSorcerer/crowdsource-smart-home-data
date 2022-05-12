import connexion
import six

from dateutil.parser import ParserError

from crowdsorcerer_server_export import util
from crowdsorcerer_server_export.hudi_utils.operations import HudiOperations
from crowdsorcerer_server_export.exceptions import BadDateFormat
# from df2ckan import convert


def data_extraction(format_, date_from=None, date_to=None):  # noqa: E501
    """Extract data from the data lake into a CKAN compliant format, zipped.

     # noqa: E501

    :param format: The desired exportation format
    :type format: str
    :param date_from: Only data from this date forwards will be extracted
    :type date_from: str
    :param date_to: Only data from this date backwards will be extracted.
    :type date_to: str

    :rtype: None
    """

    try:
        date_from = util.deserialize_date(date_from) if date_from else None
    except ParserError:
        raise BadDateFormat('date_from')

    try:
        date_to = util.deserialize_date(date_to) if date_to else None
    except ParserError:
        raise BadDateFormat('date_to')

    df = HudiOperations.get_data(
        date_from=date_from,
        date_to=date_to
    )

    # return convert(df, to=format_)
    return 'do some magic!'

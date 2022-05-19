import connexion
import six

import io
from dateutil.parser import ParserError
from zlib import compress
import zipfile

from flask import Response, send_file
from crowdsorcerer_server_export import util
from crowdsorcerer_server_export.hudi_utils.operations import HudiOperations
from crowdsorcerer_server_export.exceptions import BadDateFormat, UnsupportedExportationFormat
from crowdsorcerer_server_export.df2CKAN import EXPORT_FORMATS



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

    format_ = format_.lower()
    if format_ not in EXPORT_FORMATS:
        raise UnsupportedExportationFormat(format_)
    convert = EXPORT_FORMATS[format_]

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

    response_data = convert(df).encode(encoding='utf-8')
    
    zipped_data = io.BytesIO()
    z = zipfile.ZipFile(zipped_data, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
    z.writestr(f'crowdsorcerer_extract.{format_}', response_data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    z.close()

    print('Data:', zipped_data.read())

    return send_file(
        path_or_file=zipped_data,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=f'crowdsorcerer_extract.{format_}.zip')

import io
import zipfile
from dateutil.parser import ParserError

from flask import send_file
from crowdsorcerer_server_export import util
from crowdsorcerer_server_export.hudi_utils.operations import HudiOperations
from crowdsorcerer_server_export.exceptions import BadDateFormat, UnsupportedExportationFormat
from crowdsorcerer_server_export.df2CKAN import EXPORT_FORMATS



def extract(format_, date_from=None, date_to=None, types=None, units=None):

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
        date_to=date_to,
        types=types,
        units=units
    )

    response_data = convert(df).encode(encoding='utf-8')
    
    zipped_data = io.BytesIO()
    z = zipfile.ZipFile(zipped_data, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
    z.writestr(f'crowdsorcerer_extract.{format_}', response_data)
    z.close()

    zipped_data.seek(0)
    
    return send_file(
        path_or_file=zipped_data,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=f'crowdsorcerer_extract.{format_}.zip')
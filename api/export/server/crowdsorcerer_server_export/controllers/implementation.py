import io
import json
import zipfile
from typing import List
from dateutil.parser import ParserError
from datetime import date, datetime, timedelta, timezone

from flask import send_file
from crowdsorcerer_server_export import util
from crowdsorcerer_server_export.hudi_utils.operations import HudiOperations
from crowdsorcerer_server_export.exceptions import BadDateFormat, UnsupportedExportationFormat
from crowdsorcerer_server_export.df2CKAN import EXPORT_FORMATS



BASE_METADATA = {
    'name': 'crowdsorcerer-extract',
    'title': 'CrowdSorcerer extract',
    'author': 'CrowdSorcerer',
    'license_id': 'cc-zero',
    'notes': 'Crowdsourced smart home data collected from the CrowdSorcerer open source project. More info on https://smarthouse.av.it.pt'
}

def extract(formats: List[str], date_from=None, date_to=None, types=None, units=None):

    formats = [ f.lower() for f in formats ]
    for f in formats:
        if f not in EXPORT_FORMATS:
            raise UnsupportedExportationFormat(f)
    converters = [ EXPORT_FORMATS[f] for f in formats ]

    try:
        date_from = util.deserialize_date(date_from) if date_from else None
    except ParserError:
        raise BadDateFormat('date_from')

    try:
        date_to = util.deserialize_date(date_to) if date_to else None
    except ParserError:
        raise BadDateFormat('date_to')

    df, extraction_details = HudiOperations.get_data(
        date_from=date_from,
        date_to=date_to,
        types=types,
        units=units
    )

    now = datetime.now(tz=timezone(timedelta(hours=0)))
    yesterday = date.today() - timedelta(days=1)
    resources = [{
        'package-id': BASE_METADATA['name'],
        'url': 'upload-' + format_,
        'format': format_,
        'created': str(now),
        'last_modified': str(yesterday)
    } for format_ in formats]
    dataset_metadata = json.dumps({
        **BASE_METADATA,
        'resources': resources,
        'extras': [extraction_details]
    }).encode(encoding='utf-8')

    zipped_data = io.BytesIO()

    z = zipfile.ZipFile(zipped_data, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
    for format_, converter in zip(formats, converters):
        dataset_data = converter(df).encode(encoding='utf-8')
        z.writestr(f'crowdsorcerer_extract_{now.date()}.{format_}', dataset_data)
    z.writestr(f'crowdsorcerer_extract_{now.date()}_metadata.json', dataset_metadata)
    z.close()

    zipped_data.seek(0)
    
    return send_file(
        path_or_file=zipped_data,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=f'crowdsorcerer_extract_{now.date()}.zip')
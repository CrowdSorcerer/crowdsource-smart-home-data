import connexion
import six

import json
import zlib
from uuid import UUID
from http.client import OK

from crowdsorcerer_server_ingest import util
from crowdsorcerer_server_ingest.exceptions import BadIngestDecoding, BadJSONStructure, MalformedUUID
from crowdsorcerer_server_ingest.hudi_utils.operations import HudiOperations


def data_upload(body):  # noqa: E501
    """Upload Home data linked to an UUID

     # noqa: E501

    :param body: The Home data to be uploaded
    :type body: dict | bytes
    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """
    
    home_uuid_str = connexion.request.headers.get('Home_UUID')

    try:
        home_uuid = UUID(home_uuid_str)
    except ValueError:
        raise MalformedUUID()

    data = decompress_data(body)
    
    HudiOperations.insert_into_redis(home_uuid, data)

    return 'Successfully uploaded the data', OK


# Compression used: JSON -> UTF-8 encode -> zlib
def decompress_data(data: bytes) -> dict:
    try:
        data = zlib.decompress(data)
        data = data.decode(encoding='utf-8')
        data = json.loads(data)
    except Exception:
        raise BadIngestDecoding()

    if not isinstance(data, dict):
        raise BadJSONStructure()

    return data

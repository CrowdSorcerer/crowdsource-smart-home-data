import connexion
import six

from crowdsorcerer_server_ingest import util
from http.client import OK
from crowdsorcerer_server_ingest.exceptions import MalformedUUID
from crowdsorcerer_server_ingest.hudi_utils.operations import HudiOperations
from uuid import UUID


def data_upload(body):  # noqa: E501
    """Upload Home data linked to an UUID

     # noqa: E501

    :param body: The Home data to be uploaded
    :type body: dict | bytes
    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """
    
    home_uuid_str = connexion.request.headers.get('Home-UUID')

    try:
        home_uuid = UUID(home_uuid_str)
    except ValueError:
        raise MalformedUUID()

    HudiOperations.insert_data(body, home_uuid)

    return 'Successfully uploaded the data', OK

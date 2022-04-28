from http.client import OK
import connexion
import six

from swagger_server import util
from swagger_server.exceptions import MalformedUUID
from hudi_utils.operations import HudiOperations
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

    HudiOperations.send_data(body, home_uuid)

    return 'Successfully uploaded the data', OK

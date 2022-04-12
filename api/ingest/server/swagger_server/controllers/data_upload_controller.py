import connexion
import six

from swagger_server import util
from hudi_utils.operations import HudiOperations
from uuid import UUID
from .exceptions import MalformedUUID

def data_upload(body):  # noqa: E501
    """Upload Home data linked to an UUID

     # noqa: E501

    :param body: The Home data to be uploaded
    :type body: dict | bytes
    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """
    
    # Obtain header data
    home_uuid_str = connexion.request.headers['Home-UUID']

    try:
        home_uuid = UUID(home_uuid_str)
    except ValueError:
        raise MalformedUUID()
    
    if connexion.request.is_json:
        body = Object.from_dict(connexion.request.get_json())  # noqa: E501

    HudiOperations.send_data(body, home_uuid)

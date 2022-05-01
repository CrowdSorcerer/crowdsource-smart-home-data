import connexion
import six

from crowdsorcerer_server_ingest import util
from http.client import OK
from crowdsorcerer_server_ingest.exceptions import MalformedUUID
from crowdsorcerer_server_ingest.hudi_utils.operations import HudiOperations
from uuid import UUID


def data_deletion():  # noqa: E501
    """Clear Home data linked to an UUID

     # noqa: E501

    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """
    
    home_uuid_str = connexion.request.headers.get('Home_UUID')

    try:
        home_uuid = UUID(home_uuid_str)
    except ValueError:
        raise MalformedUUID()

    HudiOperations.delete_data(home_uuid)

    return 'Data from supplied UUID is cleared from the data lake', OK

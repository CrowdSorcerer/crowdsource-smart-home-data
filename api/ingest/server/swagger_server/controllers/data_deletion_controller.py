from http.client import OK
import connexion
import six

from swagger_server import util
from swagger_server.exceptions import MalformedUUID
from hudi_utils.operations import HudiOperations
from uuid import UUID

def data_deletion():  # noqa: E501
    """Clear Home data linked to an UUID

     # noqa: E501

    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """

    home_uuid_str = connexion.request.headers.get('Home-UUID')

    try:
        home_uuid = UUID(home_uuid_str)
    except ValueError:
        raise MalformedUUID()

    HudiOperations.delete_data(home_uuid)

    return 'Data from supplied UUID is cleared from the data lake', OK

import connexion
import six

from swagger_server import util
from hudi_utils.operations import HudiOperations
from uuid import UUID
from .exceptions import MalformedUUID

def data_deletion():  # noqa: E501
    """Clear Home data linked to an UUID

     # noqa: E501

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

    # Remove records from the data lake
    HudiOperations.delete_data(home_uuid)

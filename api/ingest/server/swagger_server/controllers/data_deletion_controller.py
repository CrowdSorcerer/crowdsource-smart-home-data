import connexion
import six

from swagger_server import util


def data_deletion(home_uuid):  # noqa: E501
    """Clear Home data linked to an UUID

     # noqa: E501

    :param home_uuid: This home&#x27;s UUID
    :type home_uuid: 

    :rtype: None
    """

    # Obtain header data
    home_uuid = connexion.request.headers['Home-UUID']

    # Remove records from the data lake
    #TODO

    return 'do some magic!'

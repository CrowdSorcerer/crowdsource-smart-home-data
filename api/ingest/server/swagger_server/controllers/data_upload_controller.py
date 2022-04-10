import connexion
import six

from swagger_server import util


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
    home_uuid = connexion.request.headers['Home-UUID']
    
    if connexion.request.is_json:
        body = Object.from_dict(connexion.request.get_json())  # noqa: E501
    
    # Save to the data lake
    #TODO

    return 'do some magic!'

from __future__ import print_function
import time
import crowdsorcerer_client
from crowdsorcerer_client.rest import ApiException
from pprint import pprint
from crowdsorcerer_client.configuration import Configuration

# create an instance of the API class
configuration = Configuration()
configuration.host = 'http://localhost:8080/api/ingest'
api_instance = crowdsorcerer_client.DataDeletionApi(crowdsorcerer_client.ApiClient(configuration))
home_uuid = '38400000-8cf0-11bd-b23e-10b96e4ef00d' # str | This home's UUID

try:
    # Clear Home data linked to an UUID
    api_instance.data_deletion(home_uuid)
except ApiException as e:
    print("Exception when calling DataDeletionApi->data_deletion: %s\n" % e)
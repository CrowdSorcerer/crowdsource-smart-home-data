# crowdsorcerer_client_ingest.DataDeletionApi

All URIs are relative to *https://smarthouse.av.it.pt/api/ingest*

Method | HTTP request | Description
------------- | ------------- | -------------
[**data_deletion**](DataDeletionApi.md#data_deletion) | **DELETE** /data | Clear Home data linked to an UUID

# **data_deletion**
> data_deletion(home_uuid)

Clear Home data linked to an UUID

### Example
```python
from __future__ import print_function
import time
import crowdsorcerer_client_ingest
from crowdsorcerer_client_ingest.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = crowdsorcerer_client_ingest.DataDeletionApi()
home_uuid = '38400000-8cf0-11bd-b23e-10b96e4ef00d' # str | This home's UUID

try:
    # Clear Home data linked to an UUID
    api_instance.data_deletion(home_uuid)
except ApiException as e:
    print("Exception when calling DataDeletionApi->data_deletion: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **home_uuid** | [**str**](.md)| This home&#x27;s UUID | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


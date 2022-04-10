# swagger_client.DataUploadApi

All URIs are relative to *https://smarthouse.av.it.pt/api/ingest*

Method | HTTP request | Description
------------- | ------------- | -------------
[**data_upload**](DataUploadApi.md#data_upload) | **POST** /data | Upload Home data linked to an UUID

# **data_upload**
> data_upload(body, home_uuid)

Upload Home data linked to an UUID

### Example
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DataUploadApi()
body = swagger_client.Object() # Object | The Home data to be uploaded
home_uuid = '38400000-8cf0-11bd-b23e-10b96e4ef00d' # str | This home's UUID

try:
    # Upload Home data linked to an UUID
    api_instance.data_upload(body, home_uuid)
except ApiException as e:
    print("Exception when calling DataUploadApi->data_upload: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **Object**| The Home data to be uploaded | 
 **home_uuid** | [**str**](.md)| This home&#x27;s UUID | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/octet-stream
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


# crowdsorcerer_client_export.DataExtractionApi

All URIs are relative to *https://smarthouse.av.it.pt/api/export*

Method | HTTP request | Description
------------- | ------------- | -------------
[**data_extraction**](DataExtractionApi.md#data_extraction) | **GET** /{format} | Extract data from the data lake into a CKAN compliant format, zipped.

# **data_extraction**
> data_extraction(format, date_from=date_from, date_to=date_to)

Extract data from the data lake into a CKAN compliant format, zipped.

### Example
```python
from __future__ import print_function
import time
import crowdsorcerer_client_export
from crowdsorcerer_client_export.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = crowdsorcerer_client_export.DataExtractionApi()
format = 'format_example' # str | The desired exportation format
date_from = '2013-10-20' # date | Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive. (optional)
date_to = '2013-10-20' # date | Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive. (optional)

try:
    # Extract data from the data lake into a CKAN compliant format, zipped.
    api_instance.data_extraction(format, date_from=date_from, date_to=date_to)
except ApiException as e:
    print("Exception when calling DataExtractionApi->data_extraction: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **format** | **str**| The desired exportation format | 
 **date_from** | **date**| Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive. | [optional] 
 **date_to** | **date**| Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive. | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


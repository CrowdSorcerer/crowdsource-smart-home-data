# crowdsorcerer_client_export.DataExtractionApi

All URIs are relative to *https://smarthouse.av.it.pt/api/export*

Method | HTTP request | Description
------------- | ------------- | -------------
[**available_formats**](DataExtractionApi.md#available_formats) | **GET** /formats | List of the available extraction formats
[**data_extraction**](DataExtractionApi.md#data_extraction) | **GET** /dataset | Extract data from the data lake into a CKAN compliant format, zipped.

# **available_formats**
> str available_formats()

List of the available extraction formats

### Example
```python
from __future__ import print_function
import time
import crowdsorcerer_client_export
from crowdsorcerer_client_export.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = crowdsorcerer_client_export.DataExtractionApi()

try:
    # List of the available extraction formats
    api_response = api_instance.available_formats()
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DataExtractionApi->available_formats: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **data_extraction**
> data_extraction(formats, date_from=date_from, date_to=date_to, types=types, units=units)

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
formats = ['formats_example'] # list[str] | The case insensitive strings representing the dataset's output formats
date_from = '2013-10-20' # date | Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive (optional)
date_to = '2013-10-20' # date | Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive (optional)
types = ['types_example'] # list[str] | Only data from these types of producer will be extracted (e.g. sensor) (optional)
units = ['units_example'] # list[str] | Only data which is represented in the specified units of measurement will be extracted (e.g. GHz) (optional)

try:
    # Extract data from the data lake into a CKAN compliant format, zipped.
    api_instance.data_extraction(formats, date_from=date_from, date_to=date_to, types=types, units=units)
except ApiException as e:
    print("Exception when calling DataExtractionApi->data_extraction: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **formats** | [**list[str]**](str.md)| The case insensitive strings representing the dataset&#x27;s output formats | 
 **date_from** | **date**| Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive | [optional] 
 **date_to** | **date**| Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive | [optional] 
 **types** | [**list[str]**](str.md)| Only data from these types of producer will be extracted (e.g. sensor) | [optional] 
 **units** | [**list[str]**](str.md)| Only data which is represented in the specified units of measurement will be extracted (e.g. GHz) | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


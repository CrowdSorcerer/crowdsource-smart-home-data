# coding: utf-8

"""
    CrowdSorcerer Export API

    The Export API for data exportation from the data lake into CKAN compliant formats. Not all formats may be supported.  # noqa: E501

    OpenAPI spec version: 0.0.1
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""

from __future__ import absolute_import

import re  # noqa: F401

# python 2 and python 3 compatibility library
import six

from crowdsorcerer_client_export.api_client import ApiClient


class DataExtractionApi(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    Ref: https://github.com/swagger-api/swagger-codegen
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    def data_extraction(self, format, **kwargs):  # noqa: E501
        """Extract data from the data lake into a CKAN compliant format, zipped.  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.data_extraction(format, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str format: The desired exportation format (required)
        :param date date_from: Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
        :param date date_to: Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.data_extraction_with_http_info(format, **kwargs)  # noqa: E501
        else:
            (data) = self.data_extraction_with_http_info(format, **kwargs)  # noqa: E501
            return data

    def data_extraction_with_http_info(self, format, **kwargs):  # noqa: E501
        """Extract data from the data lake into a CKAN compliant format, zipped.  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.data_extraction_with_http_info(format, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str format: The desired exportation format (required)
        :param date date_from: Only data from this date forwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
        :param date date_to: Only data from this date backwards will be extracted (UTC+0, in ISO 8601 format), inclusive.
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['format', 'date_from', 'date_to']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method data_extraction" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'format' is set
        if ('format' not in params or
                params['format'] is None):
            raise ValueError("Missing the required parameter `format` when calling `data_extraction`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'format' in params:
            path_params['format'] = params['format']  # noqa: E501

        query_params = []
        if 'date_from' in params:
            query_params.append(('date_from', params['date_from']))  # noqa: E501
        if 'date_to' in params:
            query_params.append(('date_to', params['date_to']))  # noqa: E501

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/zip'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/{format}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)
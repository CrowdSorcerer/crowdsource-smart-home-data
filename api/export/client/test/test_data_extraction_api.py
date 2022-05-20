# coding: utf-8

"""
    CrowdSorcerer Export API

    The Export API for data exportation from the data lake into CKAN compliant formats. Not all formats may be supported.  # noqa: E501

    OpenAPI spec version: 0.0.1
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""

from __future__ import absolute_import

import unittest

import crowdsorcerer_client_export
from crowdsorcerer_client_export.api.data_extraction_api import DataExtractionApi  # noqa: E501
from crowdsorcerer_client_export.rest import ApiException


class TestDataExtractionApi(unittest.TestCase):
    """DataExtractionApi unit test stubs"""

    def setUp(self):
        self.api = DataExtractionApi()  # noqa: E501

    def tearDown(self):
        pass

    def test_data_extraction(self):
        """Test case for data_extraction

        Extract data from the data lake into a CKAN compliant format, zipped.  # noqa: E501
        """
        pass


if __name__ == '__main__':
    unittest.main()
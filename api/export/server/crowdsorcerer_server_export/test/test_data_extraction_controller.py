# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from crowdsorcerer_server_export.test import BaseTestCase


class TestDataExtractionController(BaseTestCase):
    """DataExtractionController integration test stubs"""

    def test_data_extraction(self):
        """Test case for data_extraction

        Extract data from the data lake into a CKAN compliant format, zipped.
        """
        query_string = [('date_from', '2013-10-20'),
                        ('date_to', '2013-10-20')]
        response = self.client.open(
            '/api/export/{format}'.format(format='format_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()

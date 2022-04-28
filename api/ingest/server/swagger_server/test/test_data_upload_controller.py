# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestDataUploadController(BaseTestCase):
    """DataUploadController integration test stubs"""

    def test_data_upload(self):
        """Test case for data_upload

        Upload Home data linked to an UUID
        """
        body = Object()
        headers = [('home_uuid', '38400000-8cf0-11bd-b23e-10b96e4ef00d')]
        response = self.client.open(
            '/api/ingest/data',
            method='POST',
            data=json.dumps(body),
            headers=headers,
            content_type='application/octet-stream')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()

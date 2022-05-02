import connexion

from crowdsorcerer_server_ingest import encoder

from .hudi_utils.initialize import hudi_init
from .exceptions import MALFORMED_UUID

hudi_init()

app = connexion.App(__name__, specification_dir='./swagger/')
app.app.json_encoder = encoder.JSONEncoder
app.add_api('swagger.yaml', arguments={'title': 'CrowdSorcerer Ingest API'}, pythonic_params=True)

# Exceptions
app.add_error_handler(**MALFORMED_UUID)

import os
print('INGEST_BASE_PATH:', os.environ.get('INGEST_BASE_PATH'))
print('PYSPARK_PYTHON:', os.environ.get('PYSPARK_PYTHON'))

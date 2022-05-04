import connexion

from os import environ
from distutils.util import strtobool
from crowdsorcerer_server_ingest import encoder
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from .hudi_utils.initialize import hudi_init
from .exceptions import MALFORMED_UUID



DEBUG = bool(strtobool( environ.get('INGEST_DEBUG', 'false') ))

hudi_init()

app = connexion.App(__name__, specification_dir='./swagger/')
app.app.json_encoder = encoder.JSONEncoder
app.add_api('swagger.yaml', arguments={'title': 'CrowdSorcerer Ingest API'}, pythonic_params=True)

# Exceptions
app.add_error_handler(**MALFORMED_UUID)

# Set up rate limiter
limiter = Limiter(app.app, \
    key_func=get_remote_address, \
    default_limits=['5 per hour'], \
    default_limits_per_method=True, \
    default_limits_exempt_when=lambda: DEBUG, \
    storage_uri='memory://')



print('Set INGEST_BASE_PATH:', environ.get('INGEST_BASE_PATH'))
print('Set INGEST_DEBUG:', environ.get('INGEST_DEBUG'))
print('Set PYSPARK_PYTHON:', environ.get('PYSPARK_PYTHON'))

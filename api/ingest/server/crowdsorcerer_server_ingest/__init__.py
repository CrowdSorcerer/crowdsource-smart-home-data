from os import environ
from distutils.util import strtobool
from readline import insert_text

import connexion
# from flask_limiter import Limiter
# from flask_limiter.util import get_remote_address
from flask_cors import CORS
from flask_apscheduler import APScheduler

from crowdsorcerer_server_ingest import encoder
from crowdsorcerer_server_ingest.hudi_utils.operations import HudiOperations
from .hudi_utils.initialize import hudi_init
from .exceptions import *



app = connexion.App(__name__, specification_dir='./swagger/')
app.app.json_encoder = encoder.JSONEncoder
app.add_api('swagger.yaml', arguments={'title': 'CrowdSorcerer Ingest API'}, pythonic_params=True)

cors = CORS(app.app)

# Exceptions
app.add_error_handler(**MALFORMED_UUID)
app.add_error_handler(**BAD_INGEST_DECODING)
app.add_error_handler(**BAD_JSON_STRUCTURE)
app.add_error_handler(**INVALID_JSON_KEY)

# Set up rate limiter
# limiter = Limiter(app.app, \
#     key_func=get_remote_address, \
#     default_limits=['5 per hour'], \
#     default_limits_per_method=True, \
#     default_limits_exempt_when=lambda: True, \
#     headers_enabled=True, \
#     storage_uri='memory://')

# Periodic Hudi insertions
scheduler = APScheduler()
scheduler.init_app(app.app)
scheduler.add_job(id='insert_data', func=HudiOperations.redis_into_hudi, trigger='interval', minutes=5)

scheduler.start()



print('Environment variables set')
print('INGEST_BASE_PATH:', environ.get('INGEST_BASE_PATH'))
print('INGEST_DEBUG:', environ.get('INGEST_DEBUG'))
print('INGEST_PUSHGATEWAY_HOST:', environ.get('INGEST_PUSHGATEWAY_HOST'))
print('INGEST_PUSHGATEWAY_PORT:', environ.get('INGEST_PUSHGATEWAY_PORT'))
print('PYSPARK_PYTHON:', environ.get('PYSPARK_PYTHON'))

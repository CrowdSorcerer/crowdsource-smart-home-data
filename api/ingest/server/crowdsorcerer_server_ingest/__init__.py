from os import environ

import connexion
# from flask_limiter import Limiter
# from flask_limiter.util import get_remote_address
from flask_cors import CORS
from flask_apscheduler import APScheduler

from crowdsorcerer_server_ingest import encoder
from crowdsorcerer_server_ingest.hudi_utils.operations import HudiOperations
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
ingest_hudi_rate_minutes_default = 5
ingest_hudi_rate_minutes_str = environ.get('INGEST_HUDI_RATE_MINUTES', str(ingest_hudi_rate_minutes_default))
ingest_hudi_rate_minutes_invalid = False
try:
    ingest_hudi_rate_minutes = int(ingest_hudi_rate_minutes_str)
except ValueError:
    inget_hudi_rate_minutes_invalid = True
    ingest_hudi_rate_minutes = 0
    
if ingest_hudi_rate_minutes_invalid or not ingest_hudi_rate_minutes > 0:
    print(f'Error: value of INGEST_HUDI_RATE_MINUTES \'{ingest_hudi_rate_minutes_str}\' is not a positive integer, defaulting to {ingest_hudi_rate_minutes_default}.')
    ingest_hudi_rate_minutes = ingest_hudi_rate_minutes_default

scheduler = APScheduler()
scheduler.init_app(app.app)

scheduler.add_job(id='insert_data', func=HudiOperations.redis_into_hudi, trigger='interval', minutes=ingest_hudi_rate_minutes)

scheduler.start()



print('Environment variables set')
print('INGEST_BASE_PATH:', environ.get('INGEST_BASE_PATH'))
print('INGEST_HUDI_RATE_MINUTES:', environ.get('INGEST_HUDI_RATE_MINUTES'))
print('INGEST_PUSHGATEWAY_HOST:', environ.get('INGEST_PUSHGATEWAY_HOST'))
print('INGEST_PUSHGATEWAY_PORT:', environ.get('INGEST_PUSHGATEWAY_PORT'))
print('INGEST_REDIS_HOST:', environ.get('INGEST_REDIS_HOST'))
print('INGEST_REDIS_PORT:', environ.get('INGEST_REDIS_PORT'))
print('PYSPARK_PYTHON:', environ.get('PYSPARK_PYTHON'))

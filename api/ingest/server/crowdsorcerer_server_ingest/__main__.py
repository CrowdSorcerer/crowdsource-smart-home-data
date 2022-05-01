#!/usr/bin/env python3

import connexion

from crowdsorcerer_server_ingest import encoder

from .hudi_utils.initialize import hudi_init
from .exceptions import MALFORMED_UUID


def main():
    hudi_init()

    app = connexion.App(__name__, specification_dir='./swagger/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'CrowdSorcerer Ingest API'}, pythonic_params=True)

    # Exceptions
    app.add_error_handler(**MALFORMED_UUID)

    app.run(port=8080)


if __name__ == '__main__':
    main()

# coding: utf-8

import sys
from setuptools import setup, find_packages

NAME = "crowdsorcerer_server_export"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = [
    "connexion",
    "swagger-ui-bundle>=0.0.2"
]

setup(
    name=NAME,
    version=VERSION,
    description="CrowdSorcerer Export API",
    author_email="",
    url="",
    keywords=["Swagger", "CrowdSorcerer Export API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['swagger/swagger.yaml']},
    include_package_data=True,
    entry_points={
        'console_scripts': ['crowdsorcerer_server_export=crowdsorcerer_server_export.__main__:main']},
    long_description="""\
    The Export API for data exportation from the data lake into CKAN compliant formats. Not all formats may be supported.
    """
)

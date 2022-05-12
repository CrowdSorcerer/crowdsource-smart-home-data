# Ingest API

API meant for Smart Home data ingestion. It's also through this API that the "forget" feature can be executed, which performs deletion of records associated with a specific Smart Home (identified by an UUID).

The API's code was generated using Swagger Codegen, and the specification is available in the `openapi.yaml` file.
Both the server code and client SDK are available.

In order to install the client SDK with pip (on the `dev` branch), run the command:
```
pip3 install git+https://git@github.com/CrowdSorcerer/crowdsource-smart-home-data.git@dev#subdirectory=api/ingest/client
```

# Export API

API meant for extracting the Smart Home data ingested. The following CKAN compliant formats are supported:
- JSON (recommended)
- XML
- CSV

The datasets are zipped. The available query parameters are detailed in the `server` package.

The API's code was generated using Swagger Codegen, and the specification is available in the `openapi.yaml` file.
Both the server code and client SDK are available.

In order to install the client SDK with pip (on the `dev` branch), run the command:
```
pip3 install git+https://git@github.com/CrowdSorcerer/crowdsource-smart-home-data.git@dev#subdirectory=api/export/client
```

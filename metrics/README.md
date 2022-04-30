# Metrics

Scripts developed that obtain the platform's metrics displayed on the Dashboard.
The Bash script submits the python file `hudi_metrics.py` as a Spark job, and obtains the printed metrics.
After that, the script sends those metrics to the Pushgateway server.

These scripts were built to be run periodically, as a cron job for example.

## Requirements

- PySpark

## Usage

In order to execute the script, run the following command:
```bash
./pushgateway_push_metrics.sh <job_name> <instance_name> <hudi_path> <pushgateway_host> <pushgateway_port>
```

The following arguments are required:
- **job_name**: the name of the Prometheus job
- **instance_name**: the name of the instance that submitted these metrics
- **hudi_path**: the path to the Hudi table to analyze
- **pushgateway_host**: the host address of the Pushgateway server
- **pushgateway_port**: the port of the Pushgateway server

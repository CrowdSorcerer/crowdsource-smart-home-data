# Metrics

Scripts developed that obtain the platform's metrics which are then displayed on the Dashboard.
The Bash script submits the python file `hudi_metrics.py` as a Spark job, and obtains the printed metrics.
After that, some other metrics are appended by running commands, and finally the script sends all metrics to the Pushgateway server.

These scripts were built to be run periodically, as a cron job for example.

## Requirements

- PySpark

## Usage

In order to execute the script, run the following command (this should be the current working directory):
```bash
./pushgateway_push_metrics.sh
```

Some variable's values are specified outside of the script, in the `pushgateway_push_metrics.conf` file. An example of this configuration is present with dummy values.

The following variables have to be defined:
- **job_name**: the name of the Prometheus job
- **instance_name**: the name of the instance that submitted these metrics
- **hudi_address**: the HDFS where the Hudi table is hosted (for example, `hdfs://localhost:9000`)
- **hudi_path**: the path appended to `hudi_address`, which points to the Hudi table to analyze
- **pushgateway_host**: the host address of the Pushgateway server
- **pushgateway_port**: the port of the Pushgateway server

Example file:
```
job_name=example_job
instance_name=example_intance
hudi_address=hdfs://localhost:9000
hudi_path=/tmp/hudi_trips_cow
pushgateway_host=localhost
pushgateway_port=9091
```

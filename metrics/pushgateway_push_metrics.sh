#!/bin/bash

if [ -z "$1" -o -z "$2" -o -z "$3" -o -z "$4" -o -z "$5" ]
then
    exit 1
fi

job_name=$1
instance_name=$2
hudi_path=$3
pushgateway_host=$4
pushgateway_port=$5

metrics=$(spark-submit --packages org.apache.hudi:hudi-spark3.1.2-bundle_2.12:0.10.1,org.apache.spark:spark-avro_2.12:3.1.2 hudi_metrics.py ${hudi_path})
# Remove extra line of output that is present at the top
metrics=$(echo "$metrics" | tail -n +2)

if [ -z "$metrics" ]
then
    exit 2
fi

cat <<EOF | curl --data-binary @- http://${pushgateway_host}:${pushgateway_port}/metrics/job/${job_name}/instance/${instance_name}
${metrics}
EOF
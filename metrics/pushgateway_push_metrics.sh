#!/bin/bash

while read -r line
do
    var=$(echo "$line" | cut -f1 -d=)
    value=$(echo "$line" | cut -f2 -d=)
    declare ${var}=${value}
done < pushgateway_push_metrics.conf

if [ -z "$job_name" \
    -o -z "$instance_name" \
    -o -z "$hudi_address" \
    -o -z "$hudi_path" \
    -o -z "$pushgateway_host" \
    -o -z "$pushgateway_port" ]
then
    exit 1
fi

spark-submit --packages org.apache.hudi:hudi-spark3.1.2-bundle_2.12:0.10.1,org.apache.spark:spark-avro_2.12:3.1.2 hudi_metrics.py --hudi-location ${hudi_address}${hudi_path}
metrics=$(cat hudi_metrics.txt)



if command -v hdfs
then
    data_lake_size=$(hdfs dfs -du -s $hudi_path | cut -f1 -d ' ')
elif [ "$hudi_address" = "file://" ]
then
    data_lake_size=$(du -s $hudi_path | cut -f1)
else
    data_lake_size=-1
fi

metrics+="
# TYPE data_lake_size gauge
# HELP data_lake_size The size of the data lake, in bytes
data_lake_size ${data_lake_size}"

if [ -z "$metrics" ]
then
    exit 2
fi

echo "Pushing metrics:
$metrics" > pushed_metrics.txt

cat <<EOF | curl --data-binary @- http://${pushgateway_host}:${pushgateway_port}/metrics/job/${job_name}/instance/${instance_name}
${metrics}
EOF

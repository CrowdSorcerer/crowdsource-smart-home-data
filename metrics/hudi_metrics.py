import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession



METADATA_COLUMNS = ['uuid', 'path_year', 'path_month', 'path_day', 'ts']
HOODIE_COLUMNS = ['_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name']

def main(spark, hudi_location):
    df = spark.read.format('hudi').load(hudi_location)

    metrics_output_file = open('hudi_metrics.txt', 'w')

    n_users = df.select('uuid').distinct().count()
    print_metric('data_lake_users', 'gauge', 'The amount of users on the platform at the moment', n_users, metrics_output_file)

    max_days_without_upload = 30
    minimum_dt = datetime.now() - timedelta(days=max_days_without_upload)
    n_discontinued = df.select('uuid', 'path_year', 'path_month', 'path_day').where(f'path_year<{minimum_dt.year} OR (path_year={minimum_dt.year} AND path_month<{minimum_dt.month}) OR (path_year={minimum_dt.year} AND path_month={minimum_dt.month} AND path_day<{minimum_dt.day})').count()
    print_metric('data_lake_discontinued', 'gauge', f'The amount of discontinued data uploads (more than {max_days_without_upload} without upload)', n_discontinued, metrics_output_file)

    n_data_producers = len(df.columns) - len(METADATA_COLUMNS) - len(HOODIE_COLUMNS)
    print_metric('data_lake_producers', 'counter', 'The amount of distinct data producers (e.g. sensors) on the data lake', n_data_producers, metrics_output_file)

    metrics_output_file.close()



def print_metric(_name, _type, _help, _value, _file):
    print(
f'''# TYPE {_name} {_type}
# HELP {_name} {_help}
{_name} {_value}''',
    file=_file)



if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--hudi-location')
    args = parser.parse_args()
    if not args.hudi_location:
        exit()

    main(SparkSession.builder \
            .config(key='spark.serializer', value='org.apache.spark.serializer.KryoSerializer') \
            .appName('Hudi metrics') \
            .getOrCreate(),
        args.hudi_location)

import argparse
from datetime import datetime

from pyspark.sql import SparkSession



def main(spark, hudi_location):
    df = spark.read.format('hudi').load(hudi_location)

    # TODO: watch out for privacy concerns

    n_users = df.select('uuid').distinct().count()
    print_metric('data_lake_users', 'gauge', 'The amount of users on the platform at the moment', n_users)

    max_days_without_upload = 30
    minimum_dt = datetime.fromtimestamp( datetime.now().timestamp() - max_days_without_upload*60*60*24 )
    n_discontinued = df.select('uuid', 'path_year', 'path_month', 'path_day').where(f'path_year<{minimum_dt.year} OR (path_year={minimum_dt.year} AND path_month<{minimum_dt.month}) OR (path_year={minimum_dt.year} AND path_month={minimum_dt.month} AND path_day<{minimum_dt.day})').count()
    print_metric('data_lake_discontinued', 'gauge', f'The amount of discontinued data uploads (more than {max_days_without_upload} without upload)', n_discontinued)



def print_metric(_name, _type, _help, _value):
    print(
f'''# TYPE {_name} {_type}
# HELP {_name} {_help}
{_name} {_value}''')



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

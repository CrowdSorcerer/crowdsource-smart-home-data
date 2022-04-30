from pyspark.sql import SparkSession
import argparse

def main(spark, hudi_path):
    df = spark.read.format('hudi').load(hudi_path)
    n_users = df.select('uuid').distinct().count()
    # TODO: ONLY USE FOR TESTING
    print_metric('number_of_users', 'gauge', 'The ammount of users on the platform at the moment', n_users)

def print_metric(_name, _type, _help, _value):
    print(
f'''# TYPE {_name} {_type}
# HELP {_name} {_help}
{_name} {_value}''')

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--hudi_path')
    args = parser.parse_args()
    if not args.hudi_path:
        exit()

    main(SparkSession.builder \
            .config(key='spark.serializer', value='org.apache.spark.serializer.KryoSerializer') \
            .appName('Hudi metrics') \
            .getOrCreate(),
        args.hudi_path)

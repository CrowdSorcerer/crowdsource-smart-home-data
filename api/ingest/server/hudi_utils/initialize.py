import findspark
from pyspark.sql import SparkSession
from os import environ

def hudi_init():
    # A remote Spark link can also be specified
    spark_master = environ.get('INGEST_SPARK_MASTER', 'local')

    # Local Spark configuration
    findspark.init()
    findspark.add_packages([
        'org.apache.hudi:hudi-spark3.0.3-bundle_2.12:0.10.1',
        'org.apache.spark:spark-avro_2.12:3.0.3'
    ])
    # This SparkSession configuration will be present in the entire application, since SparkSession is a singleton
    SparkSession.builder \
        .master(spark_master) \
        .config(key='spark.serializer', value='org.apache.spark.serializer.KryoSerializer') \
        .appName('Ingest API') \
        .getOrCreate()
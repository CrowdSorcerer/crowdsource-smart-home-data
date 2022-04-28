import findspark
from pyspark.sql import SparkSession

def hudi_init():

    # Local Spark configuration
    findspark.init()
    findspark.add_packages([
        'org.apache.hudi:hudi-spark3.1.2-bundle_2.12:0.10.1',
        'org.apache.spark:spark-avro_2.12:3.1.2'
    ])
    
    # This SparkSession configuration will be present in the entire application, since SparkSession is a singleton
    SparkSession.builder \
        .config(key='spark.serializer', value='org.apache.spark.serializer.KryoSerializer') \
        .appName('Ingest API') \
        .getOrCreate()
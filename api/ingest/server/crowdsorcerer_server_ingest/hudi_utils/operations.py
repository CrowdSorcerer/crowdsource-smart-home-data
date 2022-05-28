import zlib
import json
from datetime import datetime, timezone, timedelta
from os import environ
from uuid import UUID
from urllib.error import URLError
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from py4j.java_gateway import Py4JJavaError
from prometheus_client import CollectorRegistry, Counter, push_to_gateway

from crowdsorcerer_server_ingest.exceptions import BadIngestDecoding, BadJSONStructure, InvalidJSONKey
    


# Compression used: JSON -> UTF-8 encode -> zlib
def decompress_data(data: bytes) -> dict:
    try:
        data = zlib.decompress(data)
        data = data.decode(encoding='utf-8')
        data = json.loads(data)
    except Exception:
        raise BadIngestDecoding()

    if not isinstance(data, dict):
        raise BadJSONStructure()

    return data

def valid_data_key(key: str) -> bool:
    return not key.startswith('_hoodie')



class Sensor:

    def __init__(self, entity_id: str, last_changed: str, last_updated: str, state: str, attributes: dict):
        self.entity_id = entity_id
        self.last_changed = last_changed
        self.last_updated = last_updated
        self.state = state
        self.attributes = attributes

class HudiOperations:

    SPARK = SparkSession.builder.getOrCreate()
    TABLE_NAME = 'hudi_ingestion'
    BASE_PATH = environ.get('INGEST_BASE_PATH', 'file:///tmp') + '/' + TABLE_NAME

    # This is required so that proper schema evolution can be done. In case a new column is missing, we have to include all others in the insertion
    # It's assumed that this is the only place where ingestion into this table is performed
    try:
        INGESTED_COLUMNS = { (column.name, column.dataType) for column in SPARK.read.format('hudi').load(BASE_PATH).schema.fields if valid_data_key(column.name) }
    # In case the table doesn't exist yet.
    # The specific exception is java.io.FileNotFoundException, and it would be messy to check for a Java exception here, so a generic Py4J exception is checked for
    except Py4JJavaError:
        INGESTED_COLUMNS = set()

    TIMEZONE = timezone(timedelta(hours=0))

    PUSHGATEWAY_HOST = environ.get('INGEST_PUSHGATEWAY_HOST', 'localhost')
    PUSHGATEWAY_PORT = environ.get('INGEST_PUSHGATEWAY_PORT', '9091')

    HUDI_BASE_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'path_year,path_month,path_day',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.write.markers.type': 'direct'
    }

    HUDI_METRICS_OPTIONS = {
        'hoodie.metrics.on': True,
        'hoodie.metrics.reporter.type': 'PROMETHEUS_PUSHGATEWAY',
        'hoodie.metrics.pushgateway.host': PUSHGATEWAY_HOST,
        'hoodie.metrics.pushgateway.port': PUSHGATEWAY_PORT,
        'hoodie.metrics.pushgateway.job.name': 'hudi_job',
        'hoodie.metrics.pushgateway.random.job.name.suffix': False,
        'hoodie.metrics.pushgateway.delete.on.shutdown': False
    }

    HUDI_INSERT_OPTIONS = {
        **HUDI_BASE_OPTIONS,
        #**HUDI_METRICS_OPTIONS,
        'hoodie.datasource.write.operation': 'insert',
        'hoodie.datasource.write.reconcile.schema': True
    }

    HUDI_DELETE_OPTIONS = {
        **HUDI_BASE_OPTIONS,
        'hoodie.datasource.write.operation': 'delete'
    }

    REGISTRY = CollectorRegistry()
    INGEST_COUNTER = Counter('ingest_count', 'Number of ingestion (data upload) requests that have been successfully made.', registry=REGISTRY)

    @classmethod
    def insert_data(cls, datab: bytes, uuid: UUID):

        data = decompress_data(datab)
        
        # for sensor_name, sensor_data in data.items():
            # sensor = Sensor(
            #     entity_id=sensor_data['entity_id'],
            #     last_changed=sensor_data['last_changed'],
            #     last_updated=sensor_data['last_updated'],
            #     state=sensor_data['state'],
            #     attributes=sensor_data['attributes']
            # )
            # data[sensor_name] = sensor
            # data[sensor_name.replace('.', '_')] = data.pop(sensor_name)

        # for key in data:
        #     if not valid_data_key(key):
        #         raise InvalidJSONKey()

        data['uuid'] = str(uuid)
        
        dt = datetime.now(tz=cls.TIMEZONE)
        data['path_year'] = dt.year
        data['path_month'] = dt.month
        data['path_day'] = dt.day
        data['ts'] = dt.timestamp()

        df = cls.SPARK.createDataFrame([data])

        evolve_schema = False
        
        for column in df.schema.fields:
            if (column.name, column.dataType) not in cls.INGESTED_COLUMNS:
                cls.INGESTED_COLUMNS.add((column.name, column.dataType))
                evolve_schema = True

        if evolve_schema:
            print('New attribute, evolving schema')
            for non_existent_column_name, non_existent_column_type in cls.INGESTED_COLUMNS:
                if non_existent_column_name not in df.columns:
                    df = df.withColumn(non_existent_column_name, lit(None).cast(non_existent_column_type))

        print('Inserting data into', cls.BASE_PATH)
        df.write.format('hudi') \
            .options(**cls.HUDI_INSERT_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)

        cls.INGEST_COUNTER.inc()
        try:
            push_to_gateway(f'{cls.PUSHGATEWAY_HOST}:{cls.PUSHGATEWAY_PORT}', job='ingestion_job', registry=cls.REGISTRY)
        except URLError:
            print('Could not push metrics to the Pushgateway server')



    @classmethod
    def delete_data(cls, uuid: UUID):
        
        # Make sure that this is a valid UUID, especially since it will be formatted into a SQL string
        if not isinstance(uuid, UUID):
            raise ValueError("'uuid' argument is not of type UUID")

        cls.SPARK.read \
            .format('hudi') \
            .load(cls.BASE_PATH) \
            .createOrReplaceTempView('hudi_del_snapshot')

        # The uuid should be a valid UUID at this point
        ds = cls.SPARK.sql(f'select uuid, path_year, path_month, path_day from hudi_del_snapshot where uuid="{uuid}"')
        
        if ds.count() == 0:
            return

        deletes = list(map(lambda row: tuple(row), ds.collect()))
        df = cls.SPARK.sparkContext.parallelize(deletes).toDF(['uuid', 'path_year', 'path_month', 'path_day']).withColumn('ts', lit(0.0))
        
        print('Deleting data from', cls.BASE_PATH)
        df.write.format('hudi') \
            .options(**cls.HUDI_DELETE_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)

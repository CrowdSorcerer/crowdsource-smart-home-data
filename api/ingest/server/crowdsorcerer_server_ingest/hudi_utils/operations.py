import json
import re
from uuid import UUID
from os import environ
from typing import Dict
from urllib.error import URLError
from datetime import datetime, timezone, timedelta

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from py4j.java_gateway import Py4JJavaError
from prometheus_client import CollectorRegistry, Counter, push_to_gateway

from crowdsorcerer_server_ingest.hudi_utils.initialize import hudi_init



def valid_data_key(key: str) -> bool:
    return not key.startswith('_hoodie')



class Sensor:

    def __init__(self, entity_id: str, last_changed: str, last_updated: str, state: str, attributes: str):
        self.entity_id = entity_id
        self.last_changed = last_changed
        self.last_updated = last_updated
        self.state = state
        # may be simplified?
        self.attributes = json.loads( re.sub(r'([{,])\s*(.*?):', r'\1"\2":', re.sub(r'=(.*?)([,}])', r':"\1"\2', attributes)) )



hudi_init()

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
        'hoodie.metrics.pushgateway.job.name': 'hudi_ingest_job',
        'hoodie.metrics.pushgateway.random.job.name.suffix': False,
        'hoodie.metrics.pushgateway.delete.on.shutdown': False
    }

    HUDI_INSERT_OPTIONS = {
        **HUDI_BASE_OPTIONS,
        **HUDI_METRICS_OPTIONS,
        'hoodie.datasource.write.operation': 'insert',
        'hoodie.datasource.write.reconcile.schema': True
    }

    HUDI_DELETE_OPTIONS = {
        **HUDI_BASE_OPTIONS,
        'hoodie.datasource.write.operation': 'delete'
    }

    REGISTRY = CollectorRegistry()
    INGEST_UPLOAD_COUNTER = Counter('ingest_upload_count', 'Number of ingestion (data upload) requests that have been successfully made.', registry=REGISTRY)

    REDIS_HOST = environ.get('INGEST_REDIS_HOST', 'localhost')
    REDIS_PORT = int(environ.get('INGEST_REDIS_PORT', '6379'))
    REDIS_KEY_UUID_PREFIX = 'ingest:uuid:'
    REDIS = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True,
        password=environ.get('INGEST_REDIS_PASSWORD', None))

    @classmethod
    def insert_data(cls, data: Dict[str, Dict[str, Sensor]]):
        
        if not data:
            return

        for user_data in data.values():
            user_data_keys = set(user_data.keys())
            for data_key in user_data_keys:
                # Parquet tables (the way the data is stored) don't allow for the '.' character
                data_key_normalized = data_key.replace('.', '_')
                data_value = user_data.pop(data_key)
                # If normalized key will replace a legitimate key, then ignore the normalized one
                if data_key_normalized != data_key and data_key_normalized in user_data_keys:
                    continue

                # We try to structure the data properly for easier exportation
                # If that can't be done, the data is stored as-is anyway, since this is a data lake
                if isinstance(data_value, list):
                    try:
                        data_value = [Sensor(**data_value_instance) for data_value_instance in data_value]
                    except Exception as ex:
                        print('Could not properly structure data because:', ex)
                        pass
                
                user_data[data_key_normalized] = data_value                    

        data = [{'uuid': uuid, **d} for uuid, d in data.items()]

        df = cls.SPARK.createDataFrame(data)

        for column in df.schema.fields:
            if (column.name, column.dataType) not in cls.INGESTED_COLUMNS:
                cls.INGESTED_COLUMNS.add((column.name, column.dataType))

        for non_existent_column_name, non_existent_column_type in cls.INGESTED_COLUMNS:
            if non_existent_column_name not in df.columns:
                df = df.withColumn(non_existent_column_name, lit(None).cast(non_existent_column_type))

        print('Inserting data into', cls.BASE_PATH)
        df.write.format('hudi') \
            .options(**cls.HUDI_INSERT_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)



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



    @classmethod
    def insert_into_redis(cls, uuid: UUID, data: dict):
        dt = datetime.now(tz=cls.TIMEZONE)
        data['path_year'] = dt.year
        data['path_month'] = dt.month
        data['path_day'] = dt.day
        data['ts'] = dt.timestamp()
        cls.REDIS.set(cls.REDIS_KEY_UUID_PREFIX + str(uuid), json.dumps(data))

        cls.INGEST_UPLOAD_COUNTER.inc()
        try:
            push_to_gateway(f'{cls.PUSHGATEWAY_HOST}:{cls.PUSHGATEWAY_PORT}', job='hudi_ingest_counter_job', registry=cls.REGISTRY)
        except URLError:
            print('Could not push metrics to the Pushgateway server')

    @classmethod
    def redis_into_hudi(cls):
        keys = cls.REDIS.keys(cls.REDIS_KEY_UUID_PREFIX + '*')
        data_to_insert = cls.REDIS.mget(keys)

        cls.insert_data( {k.split(':')[-1]:json.loads(v) for k,v in zip(keys, data_to_insert)} )

        for key in keys:
            cls.REDIS.delete(key)
        
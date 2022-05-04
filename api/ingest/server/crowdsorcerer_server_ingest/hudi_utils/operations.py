from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timezone, timedelta
from uuid import UUID
from os import environ

class HudiOperations:

    SPARK = SparkSession.builder.getOrCreate()
    TABLE_NAME = 'hudi_ingestion'
    BASE_PATH = environ.get('INGEST_BASE_PATH', 'file:///tmp') + '/' + TABLE_NAME

    TIMEZONE = timezone(timedelta(hours=0))

    HUDI_BASE_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'path_year,path_month,path_week,path_weekday,path_hour',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.write.markers.type': 'direct'
    }

    HUDI_INSERT_OPTIONS = {**HUDI_BASE_OPTIONS,
        'hoodie.datasource.write.operation': 'insert'
    }

    HUDI_DELETE_OPTIONS = {**HUDI_BASE_OPTIONS,
        'hoodie.datasource.write.operation': 'delete'
    }

    @classmethod
    def insert_data(cls, datab: bytes, uuid: UUID):

        data = uncompress_data(datab)
        
        data['uuid'] = str(uuid)
        
        dt = datetime.now(tz=cls.TIMEZONE)
        data['path_year'], data['path_week'], data['path_weekday'] = dt.isocalendar()
        data['path_month'] = dt.month
        data['path_hour'] = dt.hour
        data['ts'] = dt.timestamp()

        df = cls.SPARK.createDataFrame(
            data=[tuple(data.values())],
            schema=tuple(data.keys())
        )

        print('Uploading data to', cls.BASE_PATH)
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
        ds = cls.SPARK.sql(f'select uuid, path_year, path_month, path_week, path_weekday, path_hour from hudi_del_snapshot where uuid="{uuid}"')
        
        if ds.count() == 0:
            return

        deletes = list(map(lambda row: tuple(row), ds.collect()))
        df = cls.SPARK.sparkContext.parallelize(deletes).toDF(['uuid', 'path_year', 'path_month', 'path_week', 'path_weekday', 'path_hour']).withColumn('ts', lit(0.0))
        
        print('Deleting data from', cls.BASE_PATH)
        df.write.format('hudi') \
            .options(**cls.HUDI_DELETE_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)
    


# TODO: What's the compression used here? Will we even need to use this?
def uncompress_data(data: bytes) -> dict:
    return {'data': data}

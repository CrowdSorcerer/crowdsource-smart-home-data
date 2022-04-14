from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timezone, timedelta
from uuid import UUID
import pandas

class HudiOperations:

    SPARK = SparkSession.builder.getOrCreate()
    TABLE_NAME = 'hudi_ingest_api_test'
    BASE_PATH = 'file:///tmp/hudi_ingest_api_test'

    TIMEZONE = timezone(timedelta(hours=0))

    HUDI_INSERT_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'year,month,week,weekday,hour',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoodie.datasource.write.operation': 'insert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.insert.shuffle.parallelism': 2
    }

    HUDI_DELETE_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'year,month,week,weekday,hour',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoddie.datasource.write.operation': 'delete',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    @classmethod
    def send_data(cls, data, uuid: UUID):

        # TODO: data structure is not yet properly defined: what's the compression used here?
        data = { "data": data }

        data['uuid'] = str(uuid)
        
        dt = datetime.now(tz=cls.TIMEZONE)
        data['year'], data['week'], data['weekday'] = dt.isocalendar()
        data['month'], data['hour'] = dt.month, dt.hour

        df = cls.SPARK.createDataFrame(pandas.DataFrame(data, index=[0]))
        df.write.format('hudi') \
            .options(**cls.HUDI_INSERT_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)

    @classmethod
    def delete_data(cls, uuid: UUID):
        # Make sure that this is a valid UUID, especially since it will be formatted into an SQL string
        if not isinstance(uuid, UUID):
            raise ValueError("'uuid' argument is not of type UUID")

        # TODO: is this really necessary??
        df = cls.SPARK.read \
            .format('hudi') \
            .load(cls.BASE_PATH)
        df.createOrReplaceTempView('hudi_tmp_del')

        # This shouldn't pose an SQL Injection problem, right?
        # The uuid should be a valid UUID at this point
        ds = cls.SPARK.sql(f'select uuid, year, month, week, weekday, hour from hudi_tmp_del where uuid="{uuid}"')
        
        # TODO: check if tuple(row) works, which would be simpler
        deletes = list(map(lambda row: (row[0], row[1], row[2], row[3], row[4], row[5]), ds.collect()))
        df = cls.SPARK.sparkContext.parallelize(deletes).toDF(['uuid', 'year', 'month', 'week', 'weekday', 'hour']).withColumn('ts', lit(0.0))
        df.write.format('hudi') \
            .options(cls.HUDI_DELETE_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)
        
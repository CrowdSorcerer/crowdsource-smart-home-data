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

        data = {
            "data": data
        }

        data['uuid'] = str(uuid)
        
        # TODO: no time zone?
        dt = datetime.now(tz=cls.TIMEZONE)
        data['year'], data['week'], data['weekday'] = dt.isocalendar()
        data['month'], data['hour'] = dt.month, dt.hour

        # df = cls.SPARK.read.json(cls.SPARK.sparkContext.parallelize([str(data)], 2))
        df = cls.SPARK.createDataFrame(pandas.DataFrame(data, index=[0]))
        df.write.format('hudi') \
            .options(**cls.HUDI_INSERT_OPTIONS) \
            .mode('overwrite') \
            .save(cls.BASE_PATH)

    @classmethod
    def delete_data(cls, uuid: UUID):
        
        # This shouldn't pose an SQL Injection problem, right?
        # If someone could ever change the value of table_name, then there would be bigger problems, and the uuid should be a valid UUID
        ds = cls.SPARK.sql(f'select uuid, year, month, week, weekday, hour from {cls.TABLE_NAME} where uuid="{uuid}"')
        
        # TODO: check if tuple(row) works, which would be simpler
        deletes = list(map(lambda row: (row[0], row[1], row[2], row[3], row[4], row[5]), ds.collect()))
        df = cls.SPARK.sparkContext.parallelize(deletes).toDF(['uuid', 'year', 'month', 'week', 'weekday', 'hour']).withColumn('ts', lit(0.0))
        df.write.format('hudi') \
            .options(cls.HUDI_DELETE_OPTIONS) \
            .mode('append') \
            .save(cls.BASE_PATH)
        
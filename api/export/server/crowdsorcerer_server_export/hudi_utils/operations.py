from os import environ
from datetime import date

from pyspark.sql import SparkSession
from pandas import DataFrame


class HudiOperations:

    SPARK = SparkSession.builder.getOrCreate()
    TABLE_NAME = 'hudi_ingestion'
    BASE_PATH = environ.get('INGEST_BASE_PATH', 'file:///tmp') + '/' + TABLE_NAME

    HUDI_BASE_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'path_year,path_month,path_day',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.write.markers.type': 'direct',
    }

    # TODO
    @classmethod
    def get_data(cls, date_from: date=None, date_to: date=None) -> DataFrame:
        df = cls.SPARK.read.format('hudi').load(cls.BASE_PATH)

        df.drop('uuid', 'ts', '_hoodie*') \
            .where(f'path_year>={date_from.year} \
                OR (path_year={date_from.year} AND path_month>={date_from.month}) \
                OR (path_year={date_from.year} AND path_month={date_from.month} AND path_day>={date_from.day})') \
            .where(f'path_year>={date_to.year} \
                OR (path_year={date_to.year} AND path_month>={date_to.month}) \
                OR (path_year={date_to.year} AND path_month={date_to.month} AND path_day>={date_to.day})')

        return df.toPandas()

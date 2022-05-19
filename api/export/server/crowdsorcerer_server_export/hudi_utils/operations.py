from os import environ
from datetime import date

from pyspark.sql import SparkSession
from pandas import DataFrame, Series



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

    UUID_MAP = {}
    UUID_MAP_COUNTER = 0

    @classmethod
    def get_data(cls, date_from: date=None, date_to: date=None) -> DataFrame:
        df = cls.SPARK.read.format('hudi').load(cls.BASE_PATH)

        df = df.drop('ts', '_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name')

        if date_from:
            df = df.where(f'path_year>{date_from.year} \
                OR (path_year={date_from.year} AND path_month>{date_from.month}) \
                OR (path_year={date_from.year} AND path_month={date_from.month} AND path_day>={date_from.day})')
        
        if date_to:
            df = df.where(f'path_year<{date_to.year} \
                OR (path_year={date_to.year} AND path_month<{date_to.month}) \
                OR (path_year={date_to.year} AND path_month={date_to.month} AND path_day<={date_to.day})')

        # Extremelly expensive, loads everything into memory!
        dfp = df.toPandas()

        col_uuid = dfp['uuid']
        col_uuid: Series
                
        dfp['uuid'] = col_uuid.map(cls._clean_uuids)
        dfp.rename(columns={'uuid': 'id', 'path_year': 'year', 'path_month': 'month', 'path_day': 'day'}, inplace=True)

        return dfp

    @classmethod
    def _clean_uuids(cls, uuid: str):
        if uuid not in cls.UUID_MAP:
            cls.UUID_MAP[uuid] = cls.UUID_MAP_COUNTER
            cls.UUID_MAP_COUNTER += 1
        return cls.UUID_MAP[uuid]
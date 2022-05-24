from os import environ
from datetime import date, timedelta

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

    COLUMN_NAME_CONVERSION_MAP = {
        'uuid': 'id',
        'path_year': 'year',
        'path_month': 'month',
        'path_day': 'day'
    }

    @classmethod
    def get_data(cls, date_from: date=None, date_to: date=None, types: str=None) -> DataFrame:
        df = cls.SPARK.read.format('hudi').load(cls.BASE_PATH)

        df = df.drop('ts', '_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name')

        yesterday = date.today() - timedelta(days=1)
        date_to = yesterday if not date_to or date_to > yesterday else date_to

        if date_from:
            df = df.where(f'path_year>{date_from.year} \
                OR (path_year={date_from.year} AND path_month>{date_from.month}) \
                OR (path_year={date_from.year} AND path_month={date_from.month} AND path_day>={date_from.day})')
        
        df = df.where(f'path_year<{date_to.year} \
            OR (path_year={date_to.year} AND path_month<{date_to.month}) \
            OR (path_year={date_to.year} AND path_month={date_to.month} AND path_day<={date_to.day})')

        if types:
            types_clean = [ type_.split('_')[0] for type_ in types ]
            columns = { column for column in df.columns if column.split('_')[0] in types_clean }
            df = df.select( list(columns | cls.COLUMN_NAME_CONVERSION_MAP.keys()) )

        # Extremelly expensive, loads everything into memory!
        dfp = df.toPandas()

        col_uuid = dfp['uuid']
        col_uuid: Series

        dfp['uuid'] = col_uuid.map(cls._clean_uuids)
        dfp.rename(columns=cls.COLUMN_NAME_CONVERSION_MAP, inplace=True)

        return dfp

    @classmethod
    def _clean_uuids(cls, uuid: str):
        if uuid not in cls.UUID_MAP:
            cls.UUID_MAP[uuid] = cls.UUID_MAP_COUNTER
            cls.UUID_MAP_COUNTER += 1
        return cls.UUID_MAP[uuid]
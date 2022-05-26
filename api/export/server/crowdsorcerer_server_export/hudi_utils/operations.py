from os import environ
from datetime import date, timedelta
from typing import List
from functools import reduce
from operator import iand

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import from_json, regexp_replace
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

    METADATA_COLUMNS_NAMES = ['id', 'year', 'month', 'day']

    @classmethod
    def get_data(cls, date_from: date=None, date_to: date=None, types: List[str]=None, units: List[str]=None) -> DataFrame:
        df = cls.SPARK.read.format('hudi').load(cls.BASE_PATH)

        df = df \
            .drop('ts', '_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name') \
            .withColumnRenamed('uuid', 'id') \
            .withColumnRenamed('path_year', 'year') \
            .withColumnRenamed('path_month', 'month') \
            .withColumnRenamed('path_day', 'day')

        yesterday = date.today() - timedelta(days=1)
        date_to = yesterday if not date_to or date_to > yesterday else date_to

        metadata_columns = [ (df[column_name], column_name) for column_name in cls.METADATA_COLUMNS_NAMES ]
        data_columns = [ (col, name) for col, name in zip(df, df.columns) if name not in cls.METADATA_COLUMNS_NAMES ]

        if date_from:
            df = df.where(f'path_year>{date_from.year} \
                OR (path_year={date_from.year} AND path_month>{date_from.month}) \
                OR (path_year={date_from.year} AND path_month={date_from.month} AND path_day>={date_from.day})')
        
        df = df.where(f'path_year<{date_to.year} \
            OR (path_year={date_to.year} AND path_month<{date_to.month}) \
            OR (path_year={date_to.year} AND path_month={date_to.month} AND path_day<={date_to.day})')

        if types:
            types_clean = [ type_.split('_')[0] for type_ in types ]
            data_columns = [ (col, name) for col, name in data_columns if name.split('_')[0] in types_clean ]
            df = df.select([ col for col, _ in (metadata_columns + data_columns) ])

        if units:
            units_columns = [(from_json(regexp_replace(column[0].attributes, '=(.*?)([,}])', ':"$1"$2'), 'unit_of_measurement STRING', {'allowUnquotedFieldNames': True}).unit_of_measurement.alias(column_name), column_name) for column, column_name in data_columns]
            for column, column_name in units_columns:
                print('Count of nulls in ', column_name, 'is', df.select(column).dropna().count())
                df.select(column).show()
                if df.select(column).dropna().count() == 0:
                    print('Dropping ', column_name)
                    df.drop(column_name)
                else:
                    df = df.where(column.isin(units))
            data_columns = [ (col, name) for col, name in data_columns if name in df.columns ]
            

        for column, _ in data_columns:
            if df.select(column).dropna().count() == 0:
                df = df.drop(column)


        # Extremelly expensive, loads everything into memory!
        dfp = df.toPandas()

        col_id = dfp['id']
        col_id: Series

        dfp['id'] = col_id.map(cls._clean_uuids)

        print('Pandas df:', dfp)

        return dfp

    @classmethod
    def _clean_uuids(cls, uuid: str):
        if uuid not in cls.UUID_MAP:
            cls.UUID_MAP[uuid] = cls.UUID_MAP_COUNTER
            cls.UUID_MAP_COUNTER += 1
        return cls.UUID_MAP[uuid]
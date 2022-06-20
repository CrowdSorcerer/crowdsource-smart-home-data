from os import environ
from datetime import date, timedelta
from typing import Dict, List, Tuple
from functools import reduce
from operator import ior

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pandas import DataFrame, Series

from crowdsorcerer_server_export.exceptions import EmptyDataset



class HudiOperations:

    SPARK = SparkSession.builder.getOrCreate()
    TABLE_NAME = 'hudi_ingestion'
    BASE_PATH = environ.get('EXPORT_BASE_PATH', 'file:///tmp') + '/' + TABLE_NAME

    HUDI_BASE_OPTIONS = {
        'hoodie.table.name': TABLE_NAME,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'path_year,path_month,path_day',
        'hoodie.datasource.write.table.name': TABLE_NAME,
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.write.markers.type': 'direct',
    }

    uuid_map = {}
    uuid_map_counter = 0

    METADATA_COLUMNS_NAMES = ['id', 'year', 'month', 'day']

    @classmethod
    def get_data(cls, date_from: date=None, date_to: date=None, types: List[str]=None, units: List[str]=None) -> Tuple[DataFrame, Dict[str, str]]:
        df = cls.SPARK.read.format('hudi').load(cls.BASE_PATH)

        df = df \
            .drop('ts', '_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name') \
            .withColumnRenamed('uuid', 'id') \
            .withColumnRenamed('path_year', 'year') \
            .withColumnRenamed('path_month', 'month') \
            .withColumnRenamed('path_day', 'day') \

        # Limit the exported data to the data that is properly structured and can be predictably worked with
        structured_data_columns = [
            field.name for field in df.schema.fields \
            if isinstance(field.dataType.jsonValue(), dict) \
                and field.dataType.jsonValue()['type'] == 'array' \
                and field.dataType.jsonValue()['elementType']['type'] == 'struct'
        ]

        yesterday = date.today() - timedelta(days=1)
        date_to = yesterday if not date_to or date_to > yesterday else date_to

        metadata_columns = [ (df[column_name], column_name) for column_name in cls.METADATA_COLUMNS_NAMES ]
        data_columns = [ (col, name) for col, name in zip(df, df.columns) if name in structured_data_columns ]

        if date_from:
            df = df.where(f'path_year>{date_from.yecls.METADATA_COLUMNS_NAMESar} \
                OR (path_year={date_from.year} AND path_month>{date_from.month}) \
                OR (path_year={date_from.year} AND path_month={date_from.month} AND path_day>={date_from.day})')
        
        df = df.where(f'path_year<{date_to.year} \
            OR (path_year={date_to.year} AND path_month<{date_to.month}) \
            OR (path_year={date_to.year} AND path_month={date_to.month} AND path_day<={date_to.day})')

        if types:
            # Suggestion: use df.colRegex(...)?
            data_columns = [ (col, name) for col, name in data_columns if any(name.startswith(type_) for type_ in types) ]
            df = df.select([ col for col, _ in (metadata_columns + data_columns) ])

        if units:
            units_columns = [(F.from_json(F.regexp_replace(column[0].attributes, '=(.*?)([,}])', ':"$1"$2'), 'unit_of_measurement STRING', {'allowUnquotedFieldNames': True}).unit_of_measurement.alias(column_name), column_name) for column, column_name in data_columns]
            df = df.where( reduce(ior, [ column.isin(units) for column in units_columns ]) )
            data_columns = [ (col, name) for col, name in data_columns if name in df.columns ]

        redundant_columns = df.agg(*[F.min(col.isNull()).alias(name) for col, name in data_columns])\
            .first() \
            .asDict()
        df = df.drop(*[column for column, redundant in redundant_columns if redundant])
        data_columns = [ (col, name) for col, name in data_columns if name in df.columns ]

        if len(df.columns) == len(metadata_columns) or df.count() == 0:
            raise EmptyDataset()


        # Extremelly expensive, loads everything into memory!
        dfp = df.toPandas()

        col_id = dfp['id']
        col_id: Series

        dfp['id'] = col_id.map(cls._clean_uuids)

        data_columns_names = [name for _, name in data_columns]
        dfp[data_columns_names] = dfp[data_columns_names].applymap(pandas_row_list_to_dict_list)

        year_min = dfp['year'].min()
        month_min = dfp[dfp['year'] == year_min]['month'].min()
        day_min = dfp[(dfp['year'] == year_min) & (dfp['month'] == month_min)]['day'].min()
        date_min = date(year=year_min, month=month_min, day=day_min)

        return dfp, {
            'date_from': str(date_min if not date_from or date_from < date_min else date_from),
            'date_to': str(date_to),
            'types': 'any' if not types else str(types),
            'units': 'any' if not units else str(units)
        }


    @classmethod
    def _clean_uuids(cls, uuid: str):
        if uuid not in cls.uuid_map:
            cls.uuid_map[uuid] = cls.uuid_map_counter
            cls.uuid_map_counter += 1
        return cls.uuid_map[uuid]
    


def pandas_row_list_to_dict_list(elem):
    return [intraRow.asDict() for intraRow in elem] if elem else None

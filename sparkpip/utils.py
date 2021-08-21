# -*- coding: utf-8 -*-
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import when as spark_when
from pyspark.sql.functions import isnan as spark_isnan
from typing import Dict


def convert_to_null(tables: Dict) -> Dict:
    """
    Функция замена всех пустых занчений на null

    Качество данных пока вызывает вопросы, поэтому предпринимаются следующие действия:

    * В колонках со строковым типом все значения ячеек ('', 'NaN') приводятся к null
    * В колонках типов ('double', 'float') все NaN'ы приводятся к null

    Parameters
    ----------
    tables : dict
        словарь таблиц спарковских таблиц для обработки.

    Returns
    -------
    output : dict
        словарь обработанных спарковских таблиц
    """
    output = dict()
    for table_name, table in tables.items():
        dtypes = table.dtypes
        # числовые колонки
        columns_double_type = []
        # строковые
        columns_str_type = []

        for col, dtype in dtypes:
            if dtype in ('double', 'float'):
                columns_double_type.append(col)
            if dtype == 'string':
                columns_str_type.append(col)

        # преобразования NaN -> null
        for col in columns_double_type:
            table = (
                table
                .withColumn(
                    col,
                    spark_when(spark_isnan(spark_col(col)), None)
                    .otherwise(spark_col(col))
                )
            )

        # преобразования ('', 'NaN', 'nan') -> null
        for col in columns_str_type:
            table = (
                table
                .withColumn(
                    col,
                    spark_when(
                        (spark_col(col) == '')
                        | (spark_col(col) == 'NaN')
                        | (spark_col(col) == 'nan'),
                        None
                    ).otherwise(spark_col(col))
                )
            )

        output[table_name] = table

    return output
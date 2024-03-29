"""
Утилиты для модулей
"""
# -*- coding: utf-8 -*-
import os
import codecs
from typing import Dict
import yaml
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import when as spark_when
from pyspark.sql.functions import isnan as spark_isnan


def config_loader(path_to_config):
    """
    Загрузка файла конфига *.yml
    Parameters
    ----------
    path_to_config : str
        путь к файлу с конфигами

    Returns
    -------
    out : dict
        словарь с конфигами
    """

    if not os.path.exists(path_to_config):
        raise FileExistsError(f'Файла {path_to_config}, заданного в качестве пути к конфигу, не существует.')

    with codecs.open(path_to_config, 'r', "utf-8") as ymlfile:
        return yaml.load(ymlfile, Loader=yaml.FullLoader)


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
    output = {}
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

#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Здесь содержатся базовые классы для создания модулей:

StepBase -- предназначен для выполнения логического шага вычислений.
-------------------------------------------------------------------
В атрибуте класса ``source_tables`` задаются таблицы, названия и типы их колонок,
которые необходимы для вычисления шага.

В атрибуте класса ``output_tables`` задаются таблицы, названия и типы их колонок,
которые будут получены по результатам вычислений шага.

Атрибуты ``source_tables`` и ``output_tables`` -- словари с ключами "название таблицы" и описаниями,
содержащими поля:

* 'link': ссылка на таблицу,
  или её название в соответствии с config_sources.yml,
  или 'argument' в случае передачи таблиц в объект класса в качестве аргументов
* 'description': описание таблицы, просто может потом использоваться,
  если будем рисовать схемы выполнений пайплайнов
* 'columns': лист туплов колонок таблицы --
  [(имя колонки в исходной таблице, тип в исходной таблице, новое имя колонки, новый тип колонки), ...]

При инициализации объекта класса:

* входные таблицы проверяются на соответствие названий колонок и их типов по первым двум значениям туплов
* затем колонки переименовываются и их типы преобразовываются в соответствии с последними двумя значениями

Туплы 'columns' в описаниях таблиц ``output_tables`` не содержат последних двух значений, так как результаты
вычислений проходят только проверку на соответствие названий колонок и их типов. Преобразований типов
не производится.

Все вычисления шага производятся в функции ``_calculations``, которая должна быть определена
у каждого дочернего класса.

Таблицы, указанные в source_tables будут инициализированными спарковскими датафреймами при создании объекта класса
и записаны в словарь self.tables по ключам, соответствующим ключам source_tables.
(Колонки и типы будут соотетствиовать последним двум значениями туплов в 'columns' source_tables!!)

Вызвать вычисления объекта класса можно с помощью функции ``run()``

Способ применения:
------------------
>>> class SomeStep(StepBase):
>>>     source_tables = {
>>>         'plu_data': {
>>>             'link': 'dict_plu',
>>>             'description': 'Справочник товаров',
>>>             'columns': [
>>>                 ('plu_code', 'string', 'plu', None),
>>>                 ('plu_price', 'string', 'price', pyspark.sql.types.DoubleType()),
>>>             ]
>>>         }
>>>     }
>>>     output_tables = {
>>>         'new_plu': {
>>>             'link': None,
>>>             'description': None,
>>>             'columns': [
>>>                 ('plu', 'string'),
>>>                 ('promo_price', 'double'),
>>>             ]
>>>         }
>>>     }
>>>     def _calculations(self):
>>>         output = self.tables['plu_data'].withColumn('promo_price', 0.8 * F.col('price')).drop('price')
>>>         return {'new_plu': output}
>>> result = SomeStep(spark, config).run()
>>> result['new_plu'].show()

SqlImportBase -- предназначен для шагов, содержащих в себе sql-импорты
----------------------------------------------------------------------
Отличие этого класса от предыдущего заключается в отсуствии атрибута source_tables.
По-сути -- является упрощённой версией StepBase.
Лучше использовать только для SQL-запросов и их простейшей обработки

SqlOnlyImportBase -- предназначен для шагов, содержащих в себе ТОЛЬКО sql-импорты
----------------------------------------------------------------------
Отличие этого класса от предыдущего заключается в наличии функции формирования sql-запроса,
чтобы можно было использовать этот запрос в других sql-импортах.
Важно! На выходе из этого шага может быть только 1 таблица.

PipelineBase -- для создания последовательности вычислений из предыдущих двух классов.
-------------------------------------------------------------------------------------
Атрибут класса step_sequence является списком классов (шагов или sql-импортов,
причём все sql-импорты должны быть в начале последовательности)

Атрибут output_tables аналогичен предыдущим классам

При инициализации:

* проверяется соответствие таблиц-источников и колонок в них на каждом шаге
  с таблицами, рассчитанными на предыдущих шагах
* проверяется будут ли результаты каких-либо шагов перезаписаны на последующих шагах
* готовятся списки используемых в пайпланей таблиц источников HDFS
* готовятся списки промежуточных таблиц (результатов исполнения шагов пайплайна)
* таблицы, рассчитанные на каком-либо из шагов и используемые более чем в одном из последующих шагов
  автоматически кэшируются
* проверяется соответствие выходной таблице пайплайна таблицам, рассчитанным на шагах внутри пайплайна

Способ применения:
------------------
>>> class Pipeline(PipelineBase):
>>>     step_sequence = [
>>>         Step1,
>>>         Step2,
>>>         Step3
>>>     ]
>>>
>>>     output_tables = {
>>>         'any_step_output': {
>>>             'link': None,
>>>             'description': None,
>>>             'columns': [
>>>                 ('plu', 'string'),
>>>                 ('comp_id', 'string'),
>>>             ]
>>>         }
>>>     }
>>>
>>> result = Pipeline(spark, config).run()
>>> result['any_step_output'].show()

Считается, что логически завершённый результат работы модуля содержится в классе Pipeline каждого модуля

Правила оформления документации модуля:

Документация пайплайна должна состоять из одной короткой строки его описания

Пример оформления документации шага:
------------------------------------
>>> '''
>>> Краткое описание модуля в паре слов
>>> --обязательная пустая строка--
>>> Подробное описание модуля (можно использовать символы, интерпретируемые юпитерским маркдауном)
>>> '''

"""

import abc
import logging
import random
from datetime import datetime
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import when as spark_when
from pyspark.sql.functions import isnan as spark_isnan
from pyspark.sql.types import (StructField, StructType,
                               StringType, DoubleType,
                               IntegerType, DateType,
                               TimestampType, LongType,
                               BooleanType)
from typing import Dict, List, Tuple

LOGGER = logging.getLogger(__name__)

TYPES_MAPPING = {
    'string': StringType(),
    'double': DoubleType(),
    'int': IntegerType(),
    'date': DateType(),
    'timestamp': TimestampType(),
    'bigint': LongType(),
    'boolean': BooleanType()
}


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


class SqlOnlyImportBase(abc.ABC):
    """
    Класс для импорта таблиц с помощью sql запросов без участия pyspark

    output_table -- словарь, содержащий имя таблицы на выходе и её описание в формате:
        'link' -- путь к таблице или имя таблицы из config_sources (необязательно, можно задавать None)
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных), ...]
    """

    # Описание таблицы на выходе из шага
    output_tables = dict()

    def __init__(self, spark, config, logger=None):
        """

        Parameters
        ----------
        spark : объект спарка, создаваемый функцией load_spark из функций
        config : ConfigBase
            конфиги проекта
        logger : logger, optional (default=None)
            При None инициализируется свой логгер
        """
        if len(self.output_tables.keys()) > 1:
            raise ValueError('В описании выходных таблиц не может быть более 1 таблицы')
        else:
            name, descr = list(self.output_tables.items())[0]
            self.output_table_name = name
            self.output_table_descr = descr

        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

    @abc.abstractmethod
    def get_sql(self):
        """
        функция формирования sql-запроса, используемого в данном шаге

        Данный метод используется в методе класса run()

        Parameters
        ----------

        Returns
        -------
        sql : str
            sql-запрос вычислений данного шага
        """

    def check_output_tables(self, result_table):
        """
        Функция проверки таблиц, полученных из расчёта на соответствие описанию cls.output_tables.

        Parameters
        ----------
        result_table : dict
            словарь спарковских таблиц, полученных из расчёта

        Returns
        -------

        """
        table_columns_descr = self.output_table_descr['columns']

        for col_descr, dtype_descr in table_columns_descr:
            if (col_descr, dtype_descr) in result_table.dtypes:
                continue

            for src_col, src_type in result_table.dtypes:
                if src_col == col_descr:
                    raise ValueError(
                        'column "{}" in output table at step "{}" has type "{}" which differ from description'
                        .format(col_descr, self.__class__.__name__, src_type)
                    )

            raise ValueError(
                'there is no column "{}" in output table at step "{}"'
                .format(col_descr, self.__class__.__name__)
            )

    def run(self, cached=False) -> Dict:
        """
        Запуск алгоритмов вычислений, описанных в cls.get_sql().

        Parameters
        ----------
        cached : bool
            флаг кэширования таблиц в HDFS

        Returns
        -------
        result : dict
            словарь таблиц с результатами в соотвествии с cls.output_tables
        """
        sql = self.get_sql()
        result = self.spark.sql(sql)
        self.check_output_tables(result)
        if cached:
            result = result.cache()

        return {self.output_table_name: result}


class SqlImportBase(abc.ABC):
    """
    Класс для импорта таблиц с помощью sql запросов

    output_tables -- словарь таблиц, выдаваемых на выходе их расчёта
    каждая таблица в словарях содержит:
        'link' -- путь к таблице или имя таблицы из config_sources (необязательно, можно задавать None)
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных), ...]
    """

    # словарь таблиц на выходе
    output_tables = dict()

    @classmethod
    def get_schema(cls, table_name):
        """
        Формирование спарковской схемы таблицы из описания
        """
        if table_name in cls.output_tables:
            dtypes = cls.output_tables[table_name]['columns']
        else:
            raise KeyError('There is no table name "{}" in description!'.format(table_name))

        schema = StructType([StructField(col, TYPES_MAPPING[dtype], True) for col, dtype in dtypes])

        return schema

    def __init__(self, spark, config, logger=None):
        """

        Parameters
        ----------
        spark : объект спарка, создаваемый функцией load_spark из функций
        config : ConfigBase
            конфиги проекта
        logger : logger, optional (default=None)
            При None инициализируется свой логгер
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

    @abc.abstractmethod
    def _instructions(self):
        """
        sql-запрос, возможно, с последующими спарк-преобразованиями

        Данный метод используется в методе класса run()

        Parameters
        ----------

        Returns
        -------
        tables : dict
            словарь вычисленных таблиц с ключами, соответствующими cls.output_tables
        """

    def check_output_tables(self, tables):
        """
        Функция проверки таблиц, полученных из расчёта на соответствие описанию cls.output_tables.

        Parameters
        ----------
        tables : dict
            словарь спарковских таблиц, полученных из расчёта

        Returns
        -------

        """
        for table_name, tbl in tables.items():
            if table_name not in self.output_tables.keys():
                raise KeyError(
                    'table "{}" is not described in "output_tables" attribute of class "{}"'
                    .format(table_name, self.__class__.__name__)
                )

            table_columns_descr = self.output_tables[table_name]['columns']

            for col_descr, dtype_descr in table_columns_descr:
                if (col_descr, dtype_descr) in tbl.dtypes:
                    continue

                for src_col, src_type in tbl.dtypes:
                    if src_col == col_descr:
                        raise ValueError(
                            'column "{}" in output table "{}" at step "{}" has type "{}" which differ from description'
                            .format(col_descr, table_name, self.__class__.__name__, src_type)
                        )

                raise ValueError(
                    'there is no column "{}" in output table "{}" at step "{}"'
                    .format(col_descr, table_name, self.__class__.__name__)
                )

        for table_name in self.output_tables.keys():
            if table_name not in tables.keys():
                raise ValueError(
                    'there is no table "{}" in result of calculations of step "{}"'
                    .format(table_name, self.__class__.__name__)
                )

    def run(self, cached=False):
        """
        Запуск алгоритмов вычислений, описанных в cls._instructions().

        Parameters
        ----------
        cached : bool
            флаг кэширования таблиц в HDFS

        Returns
        -------
        result : dict
            словарь таблиц с результатами в соотвествии с cls.output_tables
        """
        result = self._instructions()
        self.check_output_tables(result)
        if cached:
            for key, table in result.items():
                result[key] = table.cache()

        return result


class StepBase(abc.ABC):
    """
    Класс произведения вычислений с таблицами Spark

    source_tables -- словарь таблиц для гарузки из hdfs и передаваемых в качестве аргументов, при создании класса
    output_tables -- словарь таблиц, выдаваемых на выходе их расчёта
    каждая таблица в словарях содержит:
        'link' -- путь к таблице или имя таблицы из config_sources
            или 'argument', если таблица передаётся в объект класса
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных, новое имя, новый тип данных (None -- без изменений типа))]
    """
    # словарь таблиц
    source_tables = dict()
    output_tables = dict()

    @classmethod
    def get_schema(cls, table_name):
        """
        Формирование спарковской схемы таблицы из описания
        """
        if table_name in cls.output_tables:
            dtypes = cls.output_tables[table_name]['columns']
        elif table_name in cls.source_tables:
            dtypes = cls.source_tables[table_name]['columns']
        else:
            raise KeyError('There is no table name "{}" in description!'.format(table_name))

        schema = StructType([StructField(col[0], TYPES_MAPPING[col[1]], True) for col in dtypes])

        return schema

    def __init__(self, spark, config, argument_tables=None, test=False, logger=None):
        """
        Parameters
        ----------
        spark : запущенный спарк
        config : ConfigBase
            конфиги проекта
        argument_tables : dict, optional (default=None)
            таблицы, передаваемые в качестве аргументов. Должны соответствовать таблицам из cls.source_table,
            имеющим 'link' == 'argument'
        test : bool
            флаг для юниттестов, делает все источники аргументами класса
        logger : logger, optional (default=None)
            При None инициализируется свой логгер
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

        # если модуль запускается в тестировании, то все таблицы передаются в качестве аргумента
        # не стоит запускать тестировочный модуль где-то кроме теста, для которого он инициализировался
        self.test = test
        if test:
            for key in self.source_tables:
                self.source_tables[key]['link'] = 'argument'

        self.tables = self.init_tables(argument_tables)

    def _raise_dtype_exception(self, table_dtypes, descr_col, table_name, is_source=True):
        """
        Функция выброса ошибки по несоответствию типов таблиц описанию их в классе.

        Применяется после того, как выснилось, что комбинация (имя колонки, тип) отсутствует в описнии,
        для выброса корректного эксепшна.

        Parameters
        ----------
        table_dtypes : list of tuples
            список (имя колонки, тип) проверяемой таблицы
        descr_col : str
            имя колонки из описания
        table_name : str
            имя проверяемой таблицы
        is_source : bool
            является ли таблица источником или выходом из расчётов
        """
        if is_source:
            e_type = 'column "{}" in source table "{}" at step "{}" has type "{}" which differ from description'
            e_name = 'there is no column "{}" in source table "{}" at step "{}"'
        else:
            e_type = 'column "{}" in output table "{}" at step "{}" has type "{}" which differ from description'
            e_name = 'there is no column "{}" in output table "{}" at step "{}"'

        for col, dtype in table_dtypes:
            if col == descr_col:
                raise ValueError(
                    e_type.format(col, table_name, self.__class__.__name__, dtype)
                )
        raise ValueError(
            e_name.format(descr_col, table_name, self.__class__.__name__)
        )

    def init_tables(self, argument_tables=None):
        """
        Инициализация спарковских таблиц в словарь tables.

        Табицы загружаются из HDFS если в source_tables указан 'link'.

        Если 'link' == 'argument', то таблица берётся из аргументов, передаваемых в класс при инициализации.

        Загруженные таблицы проверяются на соответствие названий колонок и их типов с описанием в cls.source_tables

        Из загруженных таблиц выбираются колонки, переименовываются и приводятся к
        новым типам в соответствии с cls.source_tables.

        **Все наны заменяются на Null**

        Parameters
        ----------
        argument_tables : dict
            словарь таблиц ('table_name', spark.table())

        Returns
        -------
        tables : dict
            словарь таблиц ('table_name', spark.table())
        """
        # Если среди таблиц есть столбцы с одинаковым названием и разным типом, будут вылетать ворнинги
        new_tables_dtypes = dict()

        tables = dict()
        for table_name, table_info in self.source_tables.items():
            # Если таблица уже загружена -- что-то не так
            if table_name in tables.keys():
                raise KeyError('table {} already exists in source_tables!'.format(table_name))

            # Инициализация spark-таблицы
            if table_info['link'] != 'argument':
                src_table = self.spark.table(self.config.get_table_link(table_info['link'], True))
            else:
                try:
                    src_table = argument_tables[table_name]
                except (KeyError, TypeError):
                    raise KeyError(
                        'table "{}" does not exist in arguments of step "{}"'
                            .format(table_name, self.__class__.__name__)
                    )

            src_table_dtypes = src_table.dtypes
            selects = []

            # Переименовывание столбцов и изменение типов в соответствии с описанием
            for col, dtype, new_name, new_dtype in table_info['columns']:
                # Если таблица присутствует в описании рефакторим её столбцы
                if (col, dtype) in src_table_dtypes:
                    selects.append(new_name)
                    src_table = src_table.withColumnRenamed(col, new_name)
                    if new_dtype is not None:
                        src_table = src_table.withColumn(
                            new_name,
                            spark_col(new_name).cast(new_dtype)
                        )
                # Если колонки из описания нет в таблице -> error
                else:
                    self._raise_dtype_exception(src_table_dtypes,
                                                col,
                                                table_name,
                                                is_source=True)

            tables[table_name] = src_table.select(selects)

            # Проверка соответствия типов колонок с одинаковыми названиями в разных таблицах
            for col, dtype in tables[table_name].dtypes:
                if col not in new_tables_dtypes.keys():
                    new_tables_dtypes[col] = {'table': table_name, 'dtype': dtype}
                else:
                    if new_tables_dtypes[col]['dtype'] != dtype:
                        self.logger.debug(
                            'columns "%s" in source tables "%s" and "%s" have different types',
                            col, table_name, new_tables_dtypes[col]['table']
                        )

        return convert_to_null(tables)

    @abc.abstractmethod
    def _calculations(self) -> Dict:
        """
        Вычисление итоговых таблиц, указанных в cls.output_tables

        Данный метод используется в методе класса run()

        Parameters
        ----------

        Returns
        -------
        tables : dict
            словарь вычисленных таблиц
        """

    def check_output_tables(self, tables: Dict):
        """
        Функция проверки таблиц, полученных из расчёта на соответствие описанию cls.output_tables.

        Parameters
        ----------
        tables : dict
            словарь спарковских таблиц, полученных из расчёта

        Returns
        -------

        """
        for table_name, tbl in tables.items():
            if table_name not in self.output_tables.keys():
                raise KeyError(
                    'table "{}" does not exist in "output_tables" attribute of class "{}"'
                        .format(table_name, self.__class__.__name__)
                )

            table_columns = self.output_tables[table_name]['columns']

            for col, dtype in table_columns:
                if (col, dtype) not in tbl.dtypes:
                    self._raise_dtype_exception(tbl.dtypes,
                                                col,
                                                table_name,
                                                is_source=False)

        for table_name in self.output_tables.keys():
            if table_name not in tables.keys():
                raise ValueError(
                    'there is no table "{}" in result of calculations of step "{}"'
                    .format(table_name, self.__class__.__name__)
                )

    def run(self, cached=False):
        """
        Запуск алгоритмов вычислений, описанных в cls._calculations().

        Parameters
        ----------
        cached : bool
            флаг кэширования таблиц в HDFS

        Returns
        -------
        result : dict
            словарь таблиц с результатами в соотвествии с cls.output_tables
        """
        result = self._calculations()
        self.check_output_tables(result)
        if cached:
            for key, table in result.items():
                result[key] = table.cache()

        return result


class PipelineBase(abc.ABC):
    """
    Класс для последовательного выполнения шагов.

    output_tables -- словарь таблиц, выдаваемых на выходе из расчёта
    каждая таблица в словаре содержит:
        'link' -- путь к таблице или 'argument', если то таблица передаётся в объект класса
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных)]
    """
    output_tables = dict()
    step_sequence = []

    def __init__(self, spark, config,
                 step_sequence=None,
                 logger=None,
                 test_arguments=None,
                 skip_structure_check=False,
                 ):
        """
        Parameters
        ----------
        spark : запущенный спарк
        config : ConfigBase
            конфиги проекта
        step_sequence : dict, optional (default=cls.step_sequence)
            список шагов пайплайна
        logger : logger, optional (default=None)
            При None инициализируется свой логгер
        test_arguments: dict, optional (default={})
            словарь таблиц аргументов шагов для тестирования. Передавать только для тестирования!
        skip_structure_check: bool, optional (default=False)
            При тестах может появиться необходимость проверить работу частичного пайплана
            (который будет начинаться с шагов, принимающих таблицы в качестве аргументов).
            Такой пайплайн не пройдёт проверку на структуру. Чтобы её пропустить, задать параметр ``True``.
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

        if step_sequence is not None:
            self.step_sequence = step_sequence

        if test_arguments is None:
            self.test = False
            self.argument_tables = {}
        else:
            self.test = True
            self.argument_tables = test_arguments

        # Список шагов, результаты которых используются в нескольких последующих шагов. Эти таблицы будем кэшировать
        self.cached_steps = []

        # Проходимся по вычислениям пайплайна, ищем невязки и шаги, которые нужно кэшировать.
        if not skip_structure_check:
            self.source_tables, self.intermediate_tables = self._check_pipeline_structure()

        # Место для записи результатов вычислений пайплайна
        self.result = dict()

    def _raise_dtype_exception(self,
                               table_dtypes: List[tuple],
                               descr_col: str,
                               table_name: str,
                               cur_step: str,
                               prev_step: str,
                               ):
        """
        Функция выброса ошибки по несоответствию колонок таблиц-фргументов колонкам таблиц,
        расчитанных на предыдущих шагах.

        Применяется после того, как выснилось, что комбинация (имя колонки, тип) проверяемой таблицы
        отсутствует в предыдущих расчётах, для выброса корректного эксепшна.

        Parameters
        ----------
        table_dtypes : list of tuples
            список (имя колонки, тип) проверяемой таблицы
        descr_col : str
            имя колонки из описания
        table_name : str
            имя проверяемой таблицы
        cur_step : str
            Имя текущего шага (для которого проверяется соответствие таблицы)
        prev_step : str
            Имя шага, на котором была рассчитана проверяемая таблица-аргумент
        """
        e_type = 'column "{}" in source table "{}" of step "{}" has type "{}" \
which differ from result, calculated at step "{}"'
        e_name = 'column "{}" of source table "{}" of step "{}" is not calculated at previous steps'

        for col, dtype in table_dtypes:
            if col == descr_col:
                raise ValueError(
                    e_type.format(col, table_name, cur_step, dtype, prev_step)
                )
        raise ValueError(
            e_name.format(descr_col, table_name, cur_step)
        )

    def _check_pipeline_structure(self) -> Tuple[Dict, Dict]:
        """
        Функция формирования информации о таблицах, используемых шагами пайплайна.

        Returns
        -------
        pip_source_tables : dict
            информация о таблицах, загружаемых из HDFS
        pip_intermediate_tables : dict
            информация о таблицах, рассчитанных на промежуточных шагах
        """
        # Создание списка источников HDFS
        pip_source_tables = dict()
        # Создание списка промежуточных таблиц
        pip_intermediate_tables = dict()

        for step in self.step_sequence:
            step_base_name = step.__bases__[0].__name__
            if step_base_name == 'StepBase':
                step_source_tables = step.source_tables
            elif step_base_name in ['SqlImportBase', 'SqlOnlyImportBase']:
                step_source_tables = dict()
            else:
                raise ValueError('Can not recognize step base class "{}"'.format(step_base_name))

            step_output_tables = step.output_tables

            # Обновление и проверка на корректность списка HDFS источников шагов пайплайна
            for table_name, table_info in step_source_tables.items():
                if table_info['link'] == 'argument':
                    continue

                if table_name not in pip_source_tables.keys():
                    pip_source_tables[table_name] = {
                        'link': table_info['link'],
                        'columns': [(col, dtype) for col, dtype, _, _ in table_info['columns']]
                    }
                    continue

                # предупреждение, если похожие названия таблиц-источников имеют разные ссылки
                if table_info['link'] != pip_source_tables[table_name]['link']:
                    self.logger.debug(
                        'source table "%s" at step "%s" has link "%s" which is differ from pipeline link "%s"',
                        table_name, step.__name__, table_info['link'], pip_source_tables[table_name]['link']
                    )

                for col, dtype, _, _ in table_info['columns']:
                    if (col, dtype) not in pip_source_tables[table_name]['columns']:
                        pip_source_tables[table_name]['columns'].append((col, dtype))

            # Проверка на корректность списка аргументов шагов (они же промежуточные шаги пайплайна)
            for table_name, table_info in step_source_tables.items():
                if table_info['link'] != 'argument':
                    continue

                # Проверяем, расчитывался ли аргумент шага на предыдущих шагах
                if table_name not in pip_intermediate_tables.keys():
                    raise ValueError(
                        'source table "{}" of step "{}" is not calculated at previous steps!'
                            .format(table_name, step.__name__)
                    )

                # Кэшируем таблицу, если она используется неоднократно
                if pip_intermediate_tables[table_name]['used'] and \
                        pip_intermediate_tables[table_name]['step'] not in self.cached_steps:
                    self.cached_steps.append(pip_intermediate_tables[table_name]['step'])
                    self.logger.debug(
                        'results from step "%s" will be cached',
                        pip_intermediate_tables[table_name]['step']
                    )
                else:
                    pip_intermediate_tables[table_name]['used'] = True

                # Проверяем на соответствие таблицу аргумента шага с таблицей, которая расчитана на предыдущем шаге
                for col, dtype, _, _ in table_info['columns']:
                    if (col, dtype) not in pip_intermediate_tables[table_name]['columns']:
                        self._raise_dtype_exception(pip_intermediate_tables[table_name]['columns'],
                                                    col,
                                                    table_name,
                                                    step.__name__,
                                                    pip_intermediate_tables[table_name]['step'])

            # Обновление списка промежуточных таблиц
            for table_name, table_info in step_output_tables.items():
                if table_name in pip_intermediate_tables.keys():
                    if pip_intermediate_tables[table_name]['used']:
                        self.logger.debug(
                            'table "%s" calculated at step "%s" will be overwritten at step "%s"',
                            table_name, pip_intermediate_tables[table_name]['step'], step.__name__
                        )
                    else:
                        self.logger.debug(
                            'table "%s" calculated at step "%s" will be never used and overwritten at step "%s"!',
                            table_name, pip_intermediate_tables[table_name]['step'], step.__name__
                        )

                pip_intermediate_tables[table_name] = {
                    'step': step.__name__,
                    'used': False,
                    'columns': table_info['columns']
                }

        # Проверка выходной таблицы
        last_step_tables = self.step_sequence[-1].output_tables
        for table_name, table_info in self.output_tables.items():
            if table_name in last_step_tables.keys():
                columns = last_step_tables[table_name]['columns']
            elif table_name in pip_intermediate_tables.keys():
                columns = pip_intermediate_tables[table_name]['columns']
                self.logger.debug(
                    'pipeline output table "%s" is calculated at intermediate step (not last)',
                    table_name
                )
            else:
                raise KeyError('pipeline output table "{}" is not calculated'.format(table_name))

            for col, dtype in table_info['columns']:
                if (col, dtype) not in columns:
                    raise KeyError('pipeline output table "{}" column "{}" is not calculated'
                                   .format(table_name, col))

        return pip_source_tables, pip_intermediate_tables

    def get_pipeline_tables(self) -> Tuple[Dict, Dict, Dict]:
        """
        Выдача описания таблиц, используемых в пайплайне

        Returns
        -------
        self.source_tables : dict
            информация о таблицах, загружаемых из HDFS
        self.intermediate_tables : dict
            информация о таблицах, рассчитанных на промежуточных шагах
        self.output_tables : dict
            информация о таблицах, выдаваемых в результате запуска функции self.run()
        """

        def copy_dict_with_link(tables_dict):
            output = {}
            for key, descr in tables_dict.items():
                output[key] = {}
                for deep_key, value in tables_dict[key].items():
                    if deep_key == 'link':
                        output[key][deep_key] = self.config.get_table_link(tables_dict[key][deep_key], True)
                    else:
                        output[key][deep_key] = tables_dict[key][deep_key]
            return output

        return copy_dict_with_link(self.source_tables), \
               copy_dict_with_link(self.intermediate_tables), \
               copy_dict_with_link(self.output_tables)

    def get_pipeline_description(self, print_table_list=True, print_table_descr=True) -> str:
        """
        Функция вывода описания пайплайна для wiki

        Вывод производится в стиле юпитерского маркдауна потому, что наше вики маркдауна не знает.

        Parameters
        ----------
        print_src_tables : bool, optional (default=True)
            Вывод таблиц-источников для каждого шага
        print_out_tables : bool, optional (default=True)
            Вывод таблиц-результатов для каждого шага
        Returns
        -------
        documentation : str
            описание алгоритмов пайплайна
        """

        def get_table_descriptions(tables_description: Dict,
                                   prev_results: Dict,
                                   print_tables: bool,
                                   tab='',
                                   ) -> str:
            """
            Генерация описания таблиц
            """
            description = ''
            for table_name, table_descr in tables_description.items():
                description += tab + '* {}'.format(table_name)
                if table_name in prev_results.keys():
                    description += ' (Результат вычислений шага {})'.format(prev_results[table_name])
                elif table_descr['link'] is not None:
                    description += ' ({})'.format(self.config.get_table_link(table_descr['link'], True))

                if table_descr['description']:
                    description += ' ({})'.format(table_descr['description'])

                if print_tables:
                    description += ':\n\n' + tab + '<table>\n' + tab + '  <thead>\n'
                    description += tab + '  <tr>\n' + tab + \
                                   '    <th>Название колонки</th><th>Формат</th>\n' + tab + '  </tr>\n'

                    description += tab + '  </thead>\n' + tab + '  <tbody>\n'
                    table_row = tab + '  <tr>\n' + tab + '    <td>{}</td><td>{}</td>\n' + tab + '  </tr>\n'

                    for column in table_descr['columns']:
                        description += table_row.format(column[0], column[1])

                    description += tab + '  </tbody>\n' + tab + '</table>\n\n'
                else:
                    description += '\n'

            return description + '\n'

        # Головное описание пайплайна
        head_descriprion = self.__doc__.strip().split('\n')
        documentation = '# ' + head_descriprion[0] + '\n\n'
        for line in head_descriprion[min(1, len(head_descriprion)):]:
            line_strip = line.strip()
            if line_strip:
                documentation += line_strip + '\n'
            else:
                documentation += '\n'
        documentation += '\n\n'
        documentation += '<img src="pipeline.png">\n\n'
        # Описание шагов
        documentation += '## Модуль состоит из последовательного выполнения следующих шагов:\n'
        intermediate_results = dict()
        for step in self.step_sequence:
            # Название шага (первая строка в докстринге шага)
            description = step.__doc__.strip() + '\n'
            documentation += '* **{}** ({}):\n'.format(step.__name__, description.split('\n')[0])

            # Описание алгоритма шага, указанное в дальнейших строках докстринга
            documentation += '\n    ' + description[description.find('\n'):].strip() + '\n'
            table_list_tab = '\t'

            # Перечисление исходных таблиц, если таковые имеются
            basename = step.__bases__[0].__name__
            if basename not in ['SqlImportBase', 'SqlOnlyImportBase'] and print_table_list:
                documentation += '\n    *Исходные таблицы:*\n\n'
                documentation += get_table_descriptions(step.source_tables, intermediate_results,
                                                        print_table_descr, table_list_tab)
            else:
                documentation += '\n'

            if print_table_list:
                documentation += '    *Выходные таблицы:*\n\n'
                documentation += get_table_descriptions(step.output_tables, intermediate_results,
                                                        print_table_descr, table_list_tab)

            results = {name: step.__name__ for name in step.output_tables}
            intermediate_results = {**intermediate_results, **results}

        documentation += '\n## Результатом выполнения алгоритма являются следующие таблицы:\n'
        documentation += get_table_descriptions(self.output_tables, intermediate_results, True)

        return documentation

    def get_pipeline_graph(self) -> str:
        """
        Создание вычислительного графа пайплайна.

        Для сбора графа вывод функции можно записать в файл pipeline.dot,
        а затем вызвать сборщик: dot -Tpdf pipeline.dot > pipeline.pdf
        Returns
        -------
        graph : str
            текстовое описание графа в формате DOT
        """
        # Инициализация основных частей графа
        graph = """\
digraph G{
  graph [fontname="Liberation Serif"]
  node [shape=rect, fontname="Liberation Serif"]
  """
        sources = '\n  subgraph cluster_sources{\n    label="Исходные данные"\n'
        calculations = '\n  subgraph cluster_calculations{\n    label="Вычисления"\n'
        outputs = '\n  subgraph cluster_outputs{\n    label="Результат"\n'
        tables = dict()

        for i, step in enumerate(self.step_sequence):
            # Перечисление исходных таблиц, если таковые имеются
            basename = step.__bases__[0].__name__
            if basename in ['SqlImportBase', 'SqlOnlyImportBase']:
                sources += '    step{} [label="{}", shape=ellipse]\n'.format(i, step.__name__)
                for table_name, table_descr in step.output_tables.items():
                    tables[table_name] = ('src_table_{}'.format(table_name), '"{}"'.format(table_name))
                    sources += '    {} [label={}]\n'.format(tables[table_name][0],
                                                            tables[table_name][1])
                    sources += '    step{} -> {}\n'.format(i, tables[table_name][0])
                continue

            calculations += '    step{} [label="{}", shape=ellipse]\n'.format(i, step.__name__)

            for table_name, table_descr in step.source_tables.items():
                if (table_descr['link'] != 'argument') and (table_name not in tables.keys()):
                    table_link = self.config.get_table_link(table_descr['link'], True)
                    database, table_link = table_link.split('.')
                    tables[table_name] = (
                        'src_table_{}'.format(table_name),
                        '<{}<br/>{}>'.format(database, table_link)
                    )
                    sources += '    {} [label={}]\n'.format(tables[table_name][0],
                                                            tables[table_name][1])

                calculations += '    {} -> step{}\n'.format(tables[table_name][0], i)

            for table_name, table_descr in step.output_tables.items():
                if table_name in tables.keys():
                    tables[table_name] = (
                        '{}_recalc'.format(tables[table_name][0]),
                        '"{}"'.format(table_name)
                    )
                else:
                    tables[table_name] = (
                        'interm_table_{}'.format(table_name),
                        '"{}"'.format(table_name)
                    )
                calculations += '    {} [label={}]\n'.format(tables[table_name][0],
                                                             tables[table_name][1])
                calculations += '    step{} -> {}\n'.format(i, tables[table_name][0])

        for table_name, table_descr in self.output_tables.items():
            outputs += '    output_table_{} [label="{}"]\n'.format(table_name, table_name)
            outputs += '    {} -> output_table_{}\n'.format(tables[table_name][0], table_name)

        sources += '  }\n'
        calculations += '  }\n'
        outputs += '  }\n'
        graph += '\n'.join([sources, calculations, outputs]) + '}'

        return graph

    def _make_output_tables(self, tables) -> Dict:
        """
        Функция приведения таблиц к нормальному виду.

        * Столбцы приводятся в соответствии с ``output_tables`` по порядку колонок
        * Все NaN'ы приводятся к None

        Parameters
        ----------
        tables : dict
            словарь таблиц, расчитанных на всех шагах пайплайна.

        Returns
        -------
        output : dict
            словарь спарковских таблиц, соответствующих ``output_tables``
        """
        output = dict()
        for table_name, table_descr in self.output_tables.items():
            current_table = tables[table_name]
            # колонки для селекта
            selected_columns = [col for col, _ in table_descr['columns']]
            # выбор колонок в соответствии с output_tables
            output[table_name] = current_table.select(*selected_columns)

        return convert_to_null(output)

    def run(self) -> Dict:
        """
        Функция последовательного вычисления шагов пайплайна.

        Returns
        -------
        result : dict
            словарь таблиц с результатами в соотвествии с cls.output_tables
        """
        tables = self.argument_tables
        script_start_time = datetime.now()

        for step in self.step_sequence:
            self.logger.debug('"%s" calculations start...', step.__name__)
            if step.__bases__[0].__name__ == 'StepBase':
                if step.__name__ in self.cached_steps:
                    result = step(self.spark, self.config, tables, logger=self.logger, test=self.test).run(cached=True)
                else:
                    result = step(self.spark, self.config, tables, logger=self.logger, test=self.test).run()
            else:
                result = step(self.spark, self.config, logger=self.logger).run() if ~self.test else {}

            for table_name, table in result.items():
                tables[table_name] = table

        script_end_time = datetime.now()
        self.logger.debug('Pipeline calculations start at %s', script_start_time.strftime('%Y-%m-%d: %H:%M:%S'))
        self.logger.debug('Pipeline calculations end at %s', script_end_time.strftime('%Y-%m-%d: %H:%M:%S'))
        time_delta = (script_end_time - script_start_time).seconds
        hour_delta = time_delta // 3600
        minutes_delta = (time_delta - hour_delta * 3600) // 60
        seconds_delta = (time_delta - hour_delta * 3600) % 60
        self.logger.debug('Whole time %dh %dm %ds', hour_delta, minutes_delta, seconds_delta)

        self.result = self._make_output_tables(tables)

        return self.result

    def write_dataframe_hive(self, spark_dataframe, link, mode, partitions=None, parts_n=None):
        """
        Функция записи спарковского датафрейма в HIVE

        Parameters
        ----------
        spark_dataframe : spark.DataFrame
            Датафрейм для записи в HIVE
        link : str
            полный путь к таблице
        mode : str
            как записывать таблицу
        partitions : list, optional (default=None)
            список полей, по которым будет производиться партиционирование
        parts_n : int, optional (default=None)
            количество партиций при записи
            None -- дефолтное количество, определяемое при инициализации спарка
        """
        # Проверка партиций на адекватность
        if partitions is not None:
            for part in partitions:
                if part not in spark_dataframe.columns:
                    raise ValueError('wrong partitions')

        # Запись таблицы
        self.logger.debug('start saving table {} in mode {} with partititons {}'.format(link, mode, partitions))
        if partitions is not None:
            if parts_n is not None:
                spark_dataframe = spark_dataframe.repartition(parts_n, partitions)
            else:
                spark_dataframe = spark_dataframe.repartition(*partitions)

            if mode == 'insert':
                spark_dataframe.write.insertInto(link)
            else:
                spark_dataframe.write.partitionBy(partitions).mode(mode).saveAsTable(link)
        else:
            if mode == 'insert':
                spark_dataframe.write.insertInto(link)
            else:
                spark_dataframe.write.mode(mode).saveAsTable(link)

    def write_dataframe_hive_over_tmp(self, table_name, link, mode, partitions=None, parts_n=None):
        """
        Функция записи спарковского датафрейма в HIVE через временную таблицу:

        * датафрейм ``self.result[table_name]`` записывается во временную таблицу
        * датафрейм ``self.result[table_name]`` удаляется и создаётся датафрейм из временной таблицы
        * датафрейм из временной таблицы записывается по адресу ``link`` и удаляется

        Такая последовательность осуществляется в случае, если таблица ``link`` является исходником для вычисления
        ``self.result[table_name]``

        Parameters
        ----------
        table_name : str
            Имя таблицы из словаря self.result
        link : str
            полный путь к таблице
        mode : str
            как записывать таблицу
        partitions : list, optional (default=None)
            список полей, по которым будет производиться партиционирование
        parts_n : int, optional (default=None)
            количество партиций при записи
            None -- дефолтное количество, определяемое при инициализации спарка
        """
        # Генерируем имя временной таблицы так, чтобы оно не совпадало с уже существующей таблицей
        tmp_table_link = '011_001_0002.' + self.logger.name.replace('.', '_') + \
                         '_tmp_table_' + self.config.get_date('%Y_%m_%d') + '_' + str(random.randint(1000, 9999))

        for i in range(10):
            if not self.spark.catalog._jcatalog.tableExists(tmp_table_link):
                break
            tmp_table_link = tmp_table_link[:-4] + str(random.randint(1000, 9999))
        else:
            raise KeyError('can not create tmp table after 10 attempts')

        # Записываем результат во временную таблицу
        self.logger.debug('saving result to temporary table %s', tmp_table_link)
        self.write_dataframe_hive(self.result[table_name], tmp_table_link, 'overwrite', partitions, parts_n)

        # Удаляем записанный датафрейм и создаём датафрейм из временной таблицы
        output = self.spark.table(tmp_table_link)
        del self.result[table_name]

        # Записываем данные из временной таблицы в link.
        try:
            self.write_dataframe_hive(output, link, mode, partitions, parts_n)
        except Exception as msg:
            # Удаляем временную таблицу в случаем неудачной записи.
            self.logger.debug('can not rewrite table "%s"', link)
            self.logger.debug('removing temporary table "%s"', tmp_table_link)
            self.spark.sql('DROP TABLE ' + tmp_table_link)
            raise msg

        # Удаляем временную таблицу.
        del output
        self.logger.debug('removing temporary table %s', tmp_table_link)
        self.spark.sql('DROP TABLE {}'.format(tmp_table_link))

    def save_result_to_hive(self, table_name='all', num_partitions=None, partitions=None,
                            save_mode='append', table_link=None, use_tmp_table=False):
        """
        Модуль сохранение результатов вычислений пайплайна в HIVE

        Parameters
        ----------
        table_name : str, optional (default='all')
            имя таблицы из результатов пайплайна, которую нужно записать в HIVE
            'all' -- в случае записи всех таблиц, указанных в ``output_tables``
        num_partitions : int, optional (default=None)
            количество партиций при записи
            None -- дефолтное количество, определяемое при инициализации спарка
        partitions : list, optional (default=None)
            список полей, по которым будет производиться партиционирование
        save_mode : str, optional (default='append')
            как записывать таблицу
        table_link : str, optional (default=None)
            полный путь к таблице или имя таблицы в соответствии с config_sources.
            None -- путь будет браться из описания ``output_tables``
        """
        # Засекаем время
        saving_start_time = datetime.now()
        self.logger.debug('Saving %s table from pipeline results start at %s',
                          table_name,
                          saving_start_time.strftime('%Y-%m-%d: %H:%M:%S'))

        def save_table(tbl_name, link, mode, parts_n, parts):
            """
            Функция сохранения одной таблицы из self.result
            """
            # Определяем параметры сохранения таблицы
            link = self.output_tables[tbl_name]['link'] if link is None else link
            # Определяем параметры записи при наличии таблицы в бэкапах
            try:
                _, _, default_parts, default_link = self.config.get_table_description(link)
                default_parts = None if not default_parts else default_parts
                link = default_link if len(link.split('.')) != 2 else link
                parts = default_parts if parts is None else parts
            except KeyError:
                self.logger.debug('Table "%s" description is not found in backups', link)
            # Если таблицы нет в бэкапах пытаемся получить её ссылку из конфига, иначе оставляем без изменений
            link = self.config.get_table_link(link, quiet_mode=True)

            if link is None:
                raise ValueError('there is no table link in arguments, output_tables or cfg_sources')

            # Сохраняем таблицу
            if use_tmp_table:
                self.write_dataframe_hive_over_tmp(tbl_name, link, mode, parts, parts_n)
            else:
                self.write_dataframe_hive(self.result[tbl_name], link, mode, parts, parts_n)

        # Начинаем сохранение результатов
        if table_name == 'all':
            for key in self.output_tables:
                save_table(key, table_link, save_mode, num_partitions, partitions)
        else:
            save_table(table_name, table_link, save_mode, num_partitions, partitions)

        # Выключаем таймер, выводим инфу в лог
        saving_end_time = datetime.now()
        self.logger.debug('Saving %s table from pipeline results end at %s',
                          table_name,
                          saving_end_time.strftime('%Y-%m-%d: %H:%M:%S'))
        time_delta = (saving_end_time - saving_start_time).seconds
        hour_delta = time_delta // 3600
        minutes_delta = (time_delta - hour_delta * 3600) // 60
        seconds_delta = (time_delta - hour_delta * 3600) % 60
        self.logger.debug('Whole time %dh %dm %ds', hour_delta, minutes_delta, seconds_delta)

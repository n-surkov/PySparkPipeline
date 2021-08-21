# -*- coding: utf-8 -*-
"""
Файл описания базовых классов типа ШАГ. ШАГ задаётся для описания вычислений одного логического модуля.

Шаги делятся на 3 типа:
* StepBase
* SqlOnlyImportBase
* SqlImportBase

StepBase -- предназначен для описания логического шага вычислений.
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

* входные таблицы проверяются на соответствие описанию ``source_tables``
* затем колонки переименовываются и их типы преобразовываются в соответствии с 'columns' в ``source_tables``
* полученные spark.DataFrame записываются в словарь self.tables с ключами в соответствии ``source_tables``

Туплы 'columns' в описаниях таблиц ``output_tables`` не содержат последних двух значений, так как результаты
вычислений проходят только проверку на соответствие названий колонок и их типов. Преобразований типов
не производится.

Все вычисления шага определяются в функции ``_calculations``, которая является необходимой для определения.

Вызвать вычисления объекта класса можно с помощью функции ``run()``, которая запустит вычисления ``_calculations()``,
а затем проверит таблицы на соответствие ``output_tables``

SqlImportBase -- Упрощённая версия StepBase
----------------------------------------------------------------------
Отличие этого класса от предыдущего заключается в отсуствии атрибута source_tables.

Лучше избегать применения этого шага в production

SqlOnlyImportBase -- предназначен для шагов, содержащих в себе ТОЛЬКО sql-импорты
----------------------------------------------------------------------
Отличие этого класса от предыдущего заключается в наличии функции формирования sql-запроса,
чтобы можно было использовать этот запрос в других sql-импортах.
Важно! На выходе из этого шага может быть только 1 таблица.
"""

import abc
import logging
from pyspark.sql.functions import col as spark_col
from pyspark.sql.types import (StructField, StructType,
                               StringType, DoubleType,
                               IntegerType, DateType,
                               TimestampType, LongType,
                               BooleanType)
from typing import Dict
from .utils import convert_to_null

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


class SqlOnlyImportBasePattern(abc.ABC):
    """
    Класс для импорта таблиц с помощью sql запросов

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


class StepBasePattern(abc.ABC):
    """
    Класс произведения вычислений с таблицами Spark

    source_tables -- словарь таблиц для гарузки из hdfs и передаваемых в качестве аргументов, при создании класса
    output_tables -- словарь таблиц, выдаваемых на выходе их расчёта
    каждая таблица в словаре source_tables содержит:
        'link' -- путь к таблице или имя таблицы из config_sources
            или 'argument', если таблица передаётся в объект класса
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных, новое имя, новый тип данных (None -- без изменений типа)), ..]
    Для каждой таблицы словаря output_tables отличается только поле columns:
    [(имя столбца, тип данных), ..]
    """
    # словарь таблиц
    source_tables = dict()
    output_tables = dict()

    # Если True, то все "пустые значения" входных таблиц конвернтируются в NULL (см. convert_to_null)
    fix_nulls = True

    @classmethod
    def get_schema(cls, table_name):
        """
        Формирование спарковской схемы таблицы из описания

        Parameters
        ----------
        table_name : str
            имя таблицы, соответствующее описанию

        Returns
        -------
        schema: pyspark.sql.types.StructType
            Схема таблицы, составленная по описанию
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
        config : обект типа ConfigBase
            конфиги проекта
        argument_tables : dict, optional (default=None)
            таблицы, передаваемые в качестве аргументов. Должны соответствовать таблицам из cls.source_table,
            имеющим 'link' == 'argument'
        test : bool
            если модуль запускается в тестировании, то все self.source_tables понимаются как 'argument'
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

        self.test = test

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
            if table_info['link'] != 'argument' and not self.test:
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

        return convert_to_null(tables) if self.fix_nulls else tables

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
            В случае True все выходные таблицы кэшируются

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


class SqlImportBasePattern(abc.ABC):
    """
    Крайне не рекомендуемый для использования класс. Лучше использовать:
    * SqlOnlyImportBase -- для запросов в виде чистого sql
    * StepBase -- для работы со spark.DataFrame

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

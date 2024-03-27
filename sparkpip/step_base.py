# -*- coding: utf-8 -*-
"""
Файл описания базовых классов типа ШАГ. ШАГ задаётся для описания вычислений одного логического модуля.

Шаги делятся на 2 типа:
* StepBase
* SqlOnlyImportBase

StepBase -- предназначен для описания логического шага вычислений с фиксированными входами и выходами.
-------------------------------------------------------------------
В атрибуте класса ``source_tables`` задаются таблицы, названия и типы их колонок,
которые необходимы для вычисления шага.

В атрибуте класса ``output_tables`` задаются таблицы, названия и типы их колонок,
которые будут получены по результатам вычислений шага.

Атрибуты ``source_tables`` и ``output_tables`` -- словари с ключами "название таблицы" и описаниями,
содержащими поля:

* 'link': ссылка на таблицу,
  или её alias в соответствии с config_sources.yml,
  или 'argument' в случае передачи таблиц в объект класса в качестве аргументов
* 'description': описание таблицы -- пояснение для составления документации.
* 'columns': лист туплов колонок таблицы --
  [(
    имя колонки в исходной таблице,
    тип в исходной таблице,
    новое имя колонки (опционально),
    новый тип колонки (опционально)
  ), ...]

При инициализации объекта класса:

* входные таблицы проверяются на соответствие описанию ``source_tables``
* затем колонки переименовываются и их типы преобразовываются в соответствии с 'columns' в ``source_tables``
* полученные spark.DataFrame записываются в словарь self.tables с ключами в соответствии ``source_tables``

Туплы 'columns' в описаниях таблиц ``output_tables`` не содержат последних двух значений, так как результаты
вычислений проходят только проверку на соответствие названий колонок и их типов. Преобразований типов
не производится.

Все вычисления шага определяются в функции ``_calculations``, которая является обязательной для определения.

Вызвать вычисления объекта класса можно с помощью функции ``run()``, которая запустит вычисления ``_calculations()``,
а затем проверит таблицы на соответствие ``output_tables``

SqlOnlyImportBase -- предназначен для шагов, содержащих в себе ТОЛЬКО sql-импорты
----------------------------------------------------------------------
Отличие этого класса от предыдущего заключается в:
* наличии функции формирования sql-запроса ``get_sql()``,
  чтобы можно было использовать этот запрос в других sql-импортах.
* на выходе из этого шага может быть только 1 таблица
* атрибут ``source_tables`` отсутствует
"""

import abc
import logging
import re
from typing import Dict
from .utils import convert_to_null
from .table_description_base import TableDescriptions

LOGGER = logging.getLogger(__name__)


def print_description(step_obj, source_steps: Dict = {}):
    """
    Формирование описания шага пайплайна

    Parameters
    ----------
    step_obj: объект класса шага
    source_steps: Dict, optional (default='')
            Словарь {название таблицы: название шага}, содержащий информацию об аргументах

    Returns
    -------
    documentation: str
        описание шага в текстовом формате
    """
    documentation = ''
    # Название шага (первая строка в докстринге шага)
    description = step_obj.__doc__.strip() + '\n'
    documentation += '* **{}** ({}):\n'.format(step_obj.__class__.__name__, description.split('\n')[0])

    # Описание алгоритма шага, указанное в дальнейших строках докстринга
    documentation += '\n    ' + description[description.find('\n'):].strip() + '\n\n'

    # Перечисление исходных и выходных таблиц
    documentation += step_obj._source_tables.get_description(source_steps)
    documentation += step_obj._output_tables.get_description(source_steps)
    return documentation


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
    output_tables = {}

    def __init__(self, spark, config, logger=None, **kwargs):
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
        self.spark = spark
        self.config = config

        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

        name, _ = list(self.output_tables.items())[0]
        self.output_table_name = name

        self._output_tables = TableDescriptions(self.output_tables, 'output', self.config)

        # Парсим источники из SQL запроса
        source_tables = {}
        step_name = self.__class__.__name__
        for i, source in enumerate(re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)', self.get_sql(), re.IGNORECASE)):
            source_tables[step_name + f'_source_{i}'] = {
                'link': source,
                'description': f'Источник {i} шага "{step_name}"',
                'columns': [('unknown', 'string')]
            }
        self._source_tables = TableDescriptions(source_tables, 'source', self.config)

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

    def run(self, cached=False, get_table=None) -> Dict:
        """
        Запуск алгоритмов вычислений, описанных в cls.get_sql().

        Parameters
        ----------
        cached : bool
            флаг кэширования таблиц в HDFS
        get_table: int , optional (default=None)
            тип возвращаемого результата:
            None -- Dict[spark.DataFrame]
            0 -- spark.DataFrame

        Returns
        -------
        result : dict or spark.DataFrame
            тип зависит от параметра get_table. Результат в соответствии с cls.output_tables
        """
        sql = self.get_sql()
        result = self.spark.sql(sql)
        result = self._output_tables[self.output_table_name].convert_table(result)

        if cached:
            result = result.cache()

        if get_table is None:
            output = {self.output_table_name: result}
        else:
            output = result

        return output

    def get_description(self, source_steps: Dict = {}):
        """
        Формирование описания шага пайплайна

        Parameters
        ----------
        source_steps: Dict, optional (default='')
                Словарь {название таблицы: название шага}, содержащий информацию об аргументах

        Returns
        -------
        documentation: str
            описание шага в текстовом формате
        """
        return print_description(self, source_steps)


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
        # Сперва ищем в выходных таблицах
        schema = TableDescriptions(cls.output_tables, 'output').get_schema(table_name)
        # Затем ищем в источниках
        if schema is None:
            schema = TableDescriptions(cls.source_tables, 'source').get_schema(table_name)
        # Если не нашли, то выбрасываем ошибку
        if schema is None:
            raise KeyError(f'Таблица "{table_name}" отсутствует в описании таблиц класса.')

        return schema

    def __init__(self, spark, config, argument_tables=None, test=False, logger=None, skip_loading=False):
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
        skip_loading: bool, optional (default=False)
            пропустить загрузку этап загрузки таблиц
        """
        self.spark = spark
        self.config = config
        if logger is None:
            self.logger = LOGGER
            self.config.tune_logger(self.logger)
        else:
            self.logger = logger

        self.test = test

        # Преобразуем описания класса
        # class_name = self.__class__.__name__
        self._source_tables = TableDescriptions(self.source_tables, 'source', config, test=self.test)
        self._output_tables = TableDescriptions(self.output_tables, 'output', config, test=self.test)

        # Загружаем таблицы
        if not skip_loading:
            self.init_tables(spark, argument_tables)
        else:
            self.tables = {}

    def init_tables(self, spark, argument_tables = {}):
        """
        Загрузка таблиц во внутреннюю переменную tables

        Parameters
        ----------
        spark: запущенный спарк
        argument_tables : dict, optional (default=None)
            таблицы, передаваемые в качестве аргументов. Должны соответствовать таблицам из cls.source_table,
            имеющим 'link' == 'argument'
        """
        if argument_tables is None:
            argument_tables = {}
        self.tables = self._source_tables.load_tables(spark, argument_tables)
        if self.fix_nulls:
            self.tables = convert_to_null(self.tables)

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

    def run(self, cached=False, get_table=None):
        """
        Запуск алгоритмов вычислений, описанных в cls._calculations().

        Parameters
        ----------
        cached : bool
            В случае True все выходные таблицы кэшируются
        get_table: int , optional (default=None)
            тип возвращаемого результата:
            None -- Dict[spark.DataFrame]
            >= 0 -- индекс таблицы, которую надо вернуть в формате spark.DataFrame

        Returns
        -------
        result : dict
            тип зависит от параметра get_table. Результат в соотвествии с cls.output_tables
        """
        result = self._calculations()
        result = self._output_tables.load_tables(self.spark, result)
        if cached:
            for key, table in result.items():
                result[key] = table.cache()

        if get_table is None:
            output = result
        else:
            output = list(result.values())[get_table]

        return output

    def get_description(self, source_steps: Dict = {}):
        """
        Формирование описания шага пайплайна

        Parameters
        ----------
        source_steps: Dict, optional (default='')
                Словарь {название таблицы: название шага}, содержащий информацию об аргументах

        Returns
        -------
        documentation: str
            описание шага в текстовом формате
        """
        return print_description(self, source_steps)

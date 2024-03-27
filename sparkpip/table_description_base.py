"""
Файл базового класса описания таблиц.
"""
import re
import inspect
from copy import deepcopy
from typing import Dict
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (StructField, StructType,
                               StringType, DoubleType,
                               IntegerType, DateType,
                               TimestampType, LongType,
                               BooleanType, DecimalType,
                               ShortType, ByteType,
                               ArrayType)
from .config_base import ConfigBasePattern

def TYPES_MAPPING(spark_type_string: str):
    """
    Функция перевода текстового описания типа из sparkDF.dtypes в тип pyspark.sql.types
    Parameters
    ----------
    spark_type_string: str, тип данных спарк в текстовом формате

    Returns
    -------
    spark_sql_type: pyspark.sql.types, ип данных спарк в текстовом формате pyspark.sql.types
    """
    if 'decimal' in spark_type_string:
        decimal_parameter_1, decimal_parameter_2 = re.findall(r'\d+', spark_type_string)
        return DecimalType(int(decimal_parameter_1), int(decimal_parameter_2))

    if 'array' in spark_type_string:
        inner_type = re.findall(r'array<(.+)>', 'array<int>')[0]
        return ArrayType(TYPES_MAPPING(inner_type))

    types_mapping = {
        'string': StringType(),
        'double': DoubleType(),
        'int': IntegerType(),
        'date': DateType(),
        'timestamp': TimestampType(),
        'bigint': LongType(),
        'smallint': ShortType(),
        'boolean': BooleanType(),
        'tinyint': ByteType()
    }

    return types_mapping[spark_type_string]


class TableDescriptions:
    """
    Класс для работы с описаниями всех таблиц класса
    """
    def __init__(
            self,
            tables_descriptions: Dict,
            tables_type: str,
            config: ConfigBasePattern = None,
            step_name: str = None,
            test: bool = False):
        """

        Parameters
        ----------
        tables_descriptions: Dict
            описание входов или выходов
        tables_type: str
            тип таблиц: source -- вход, output -- выход, pipeline -- описание таблиц пайплайна
        config: ConfigBasePattern
            конфиг проекта
        step_name: str, (default=None)
            Название шага. По-умолчанию получает имя вызывающего класса
        test: bool, optional (default=False)
            перевод всех ссылок на таблицы в аргумент для тестирования
        """
        # Сохраняем название класса, в котором создаётся описание
        if step_name is None:
            calling_frame = inspect.currentframe().f_back.f_locals
            if 'cls' in calling_frame:
                step_name = calling_frame.get('cls').__name__
            else:
                step_name = calling_frame.get('self', None).__class__.__name__
        self.step_name = step_name
        # Сохраняем данные о типе таблиц
        self.tables_type = tables_type
        # Составляем описания таблиц. При составлении описания выполняются базовые проверки
        self.tables = {}
        for table_name, table_descr in tables_descriptions.items():
            if table_name in self.tables:
                raise ValueError(f'Таблица "{table_name}" встречается в описании шага "{self.step_name}" более 1 раза.')
            if tables_type != 'source':
                test = False
            self.tables[table_name] = TableSignature(
                table_descr, table_name, tables_type, step_name=self.step_name, test=test)
        # Заменяем линки на полные пути при наличие конфига
        if config is not None:
            self.recover_links(config)

    def __getitem__(self, item):
        """Поведение словаря"""
        if item not in self.tables:
            raise KeyError(f'Таблицы "{item}" нет в "{self.tables_type}" описании шага "{self.step_name}"')
        return self.tables[item]

    def get_schema(self, table_name):
        """
        Формирование спарковской схемы таблицы из описания

        Parameters
        ----------
        table_name : str
            имя таблицы, соответствующее описанию

        Returns
        -------
        schema: pyspark.sql.types.StructType
            Схема таблицы, составленная по описанию. В случае отсутствия таблиц возвращается None
        """
        if table_name in self.tables:
            return self.tables[table_name].get_schema()

        return None

    def recover_links(self, config: ConfigBasePattern):
        """
        Замена алиасов на полные пути к таблицам согласно конфигу

        Parameters
        ----------
        config: ConfigBasePattern
            конфиг проекта
        """
        for description in self.tables.values():
            description.recover_link(config)

    def load_tables(self, spark, argument_tables: Dict = {}):
        """
        Инициализация спарковских таблиц в словарь tables.

        Таблицы загружаются из HDFS если в source_tables указан 'link'.

        Если 'link' == 'argument', то таблица берётся из аргументов, передаваемых в класс при инициализации.

        Если тип таблиц 'output', то все таблицы берутся из аргументов.

        Загруженные таблицы проверяются на соответствие названий колонок и их типов с описанием в cls.source_tables

        Из загруженных таблиц выбираются колонки, переименовываются и приводятся к
        новым типам в соответствии с cls.source_tables.

        Parameters
        ----------
        spark: Спарк-сессия
        argument_tables : dict
            словарь таблиц ('table_name', spark.table())

        Returns
        -------
        tables : dict
            словарь таблиц ('table_name', spark.table())
        """

        tables = {}
        for table_name, table_description in self.tables.items():
            # Инициализация spark-таблицы
            if (table_description['link'] != 'argument') and (self.tables_type == 'source'):
                tables[table_name] = table_description.load_table(spark)
            else:
                if self.step_name != 'pipeline':
                    emsg = f'Таблица "{table_name}" шага "{self.step_name}" согласно описанию '
                else:
                    emsg = f'Таблица "{table_name}" пайплайна согласно описанию '

                if self.tables_type == 'source':
                    emsg += 'должна передаваться, как аргумент, но отсутствует в аргументах.'
                else:
                    emsg += 'должна присутствовать в результатах расчёта, но отсутствует.'

                try:
                    src_table = argument_tables[table_name]
                except (KeyError, TypeError) as exception:
                    raise KeyError(emsg) from exception
                tables[table_name] = table_description.convert_table(src_table)
        return tables

    def to_dict(self):
        """Преобразование класса к словарю"""
        return deepcopy(self.tables)

    def get_description(self, source_steps: Dict = {}) -> str:
        """
        Составление описания таблицы

        Parameters
        ----------
        source_steps: Dict, optional (default='')
            Словарь {название таблицы: название шага}, содержащий информацию об аргументах

        Returns
        -------
        description: str
            описание таблицы
        """
        indent = 1
        print_tables = False
        if self.tables_type == 'source':
            description = '    *Исходные таблицы:*\n\n'
        elif self.tables_type == 'output':
            description = '    *Выходные таблицы:*\n\n'
        else:
            description = ''
            indent = 0
            print_tables = True

        for table_name, table_descr in self.tables.items():
            step_name = source_steps.get(table_name, '')
            description += table_descr.get_description(step_name, indent, print_tables)

        return description


class TableSignature:
    """
    Класс для работы с описаниями таблиц.

    На вход должно быть передано описание формата словаря со следующими полями:

    * 'link' -- путь к таблице или имя таблицы из config_sources (необязательно, можно задавать None)
    * 'description' -- описание таблицы
    * 'columns' -- информацию о столбцах в виде списка: [(имя столбца, тип данных), ...],
      где "имя столбца" и "тип данных" -- обязательные поля.
      Для таблиц-источников, помимо обязательных полей могут опционально указываться поля
      "новое название колонки" и "новый тип колонки".
      Для описания выходных таблиц пайплайна может опционально указываться поле "описание поля".
    """

    def __init__(
            self,
            table_description: Dict,
            table_name: str,
            table_type: str,
            step_name: str = None,
            test: bool = False):
        """

        Parameters
        ----------
        table_description: Dict
            описание таблицы
        table_name: str
            название таблицы в контексте описаний шага
        table_type: str
            тип таблицы: source -- вход, output -- выход, pipeline -- описание таблицы пайплайна
        step_name: str, (default=None)
            Название шага. По-умолчанию получает имя вызывающего класса
        test: bool, optional (default=False)
            перевод ссылки на таблицу в аргумент для тестирования
        """
        # Сохраняем название класса, в котором создаётся описание
        if step_name is None:
            calling_frame = inspect.currentframe().f_back
            self.step_name = calling_frame.f_locals.get('self', None).__class__.__name__
        else:
            self.step_name = step_name
        # Сохраняем название таблицы в контексте описания
        self.table_name = table_name
        # Обрабатываем тип таблицы
        if table_type not in ['source', 'output', 'pipeline']:
            raise self.error_message('Тип таблицы может принимать только значения "source", "output", "pipeline".')
        self.table_type = table_type
        # Обрабатываем путь к таблице
        if test:
            self.link = 'argument'
        else:
            if 'link' not in table_description.keys():
                raise self.error_message('В описании таблицы должно присутствовать поле "link".', KeyError)
            self.link = table_description['link'] if table_description['link'] is not None else ''
            if not (isinstance(self.link, str) or self.link is None):
                raise self.error_message('Поле "link" в описании таблицы может быть только str или None.')
            if (self.table_type == 'source') and self.link is None:
                raise self.error_message(
                    'Поле "link" в описании входной таблицы должно принимать значение отличное от None.'
                )
            if (self.table_type == 'pipeline') and self.link is None:
                raise self.error_message('Поле "link" в описании таблиц должно принимать значение отличное от None.')

        # Обрабатываем поле description
        if 'description' not in table_description.keys():
            raise self.error_message('В описании таблицы должно присутствовать поле "description".', KeyError)
        self.description = table_description['description']

        # Обрабатываем поле columns
        if 'columns' not in table_description.keys():
            raise self.error_message('В описании таблицы должно присутствовать поле "columns".', KeyError)
        self.columns = table_description['columns']
        self.check_description()

    def __getitem__(self, key):
        """
        Для обратной совместимости, так как раньше это было просто словарём
        """
        if key == 'link':
            return self.link

        if key == 'description':
            return self.description

        if key == 'columns':
            return self.columns

        attributes = ['link', 'description', 'columns']
        raise KeyError(f'В описании таблицы нет параметра "{key}". Доступный список параметров: {attributes}')

    def _get_final_schema(self):
        """
        Формирование финальной схемы после переименовывания колонок и каста типов

        Returns
        -------
        schema: List
            схема таблицы в виде в виде списка: [(имя столбца, тип данных), ...]
        """
        output = []
        for col in self.columns:
            name_src = col[0]
            type_src = col[1]
            name_new = None
            type_new = None
            if self.table_type == 'source':
                name_new = col[2] if len(col) > 2 else name_src
                type_new = col[3] if len(col) > 3 else type_src

            # Раньше нужно было задавать все 4 колонки и заполнять ненужное наллом
            name_new = name_src if name_new is None else name_new
            type_new = type_src if type_new is None else type_new

            output.append((name_new, type_new))
        return output

    def error_message(self, msg, error=ValueError):
        """Формирование сообщения об ошибке с указанием класса"""
        location = f'Ошибка в описании таблицы "{self.table_name}" шага "{self.step_name}":'
        return error(location + msg)

    def check_description(self):
        """
        Функция базовых проверок описания таблицы на корректность
        """
        # Проверка на наличие описаний
        if len(self.columns) == 0:
            raise self.error_message('В описании таблицы не указано ни одной колонки.')

        # Проверка на необходимый минимум
        wrong_columns = []
        for col in self.columns:
            if len(col) < 2:
                wrong_columns.append(str(col))
        if wrong_columns:
            msg = 'В описании колонок должно быть минимум 2 поля -- (имя столбца, тип данных).'
            msg += 'Под это требование не подходят следующие колонки:\n' + '\n'.join(wrong_columns)
            raise self.error_message(msg)

        # Проверка на правильность заполнения
        errors = ''
        column_names = {}

        for col_src, col_new in zip(self.columns, self._get_final_schema()):
            type_src = col_src[1]
            name_new = col_new[0]
            type_new = col_new[1]

            # Проверяем коллизию имён
            if name_new in column_names:
                errors += f'\nИмя колонки "{col_src}" = "{name_new}". '
                errors += f'Такое имя уже присутствует в колонке "{column_names[name_new]}"'
            else:
                column_names[name_new] = col_src

            # Проверяем правильность типов
            try:
                TYPES_MAPPING(type_src)
            except Exception:
                errors += f'\nНе удалось распознать тип "{type_src}" в описании колонки "{col_src}"'

            if type_src != type_new:
                try:
                    TYPES_MAPPING(type_new)
                except Exception:
                    errors += f'\nНе удалось распознать тип "{type_new}" в описании колонки "{col_src}"'

        if errors:
            raise self.error_message(errors)

    def convert_table(self, dataframe: DataFrame) -> DataFrame:
        """
        Функция приведения спарковского датафрейма к описанию:
        * выбираются требуемые колонки
        * приводятся типы (если указано в описании)
        * переименовываются колонки (если указано в описании)

        Parameters
        ----------
        dataframe: pyspark.sql.DataFrame
            датафрейм, который нужно преобразовать.

        Returns
        -------
        dataframe: pyspark.sql.DataFrame
            преобразованный датафрейм
        """
        # Проверяем, что колонки соответствуют описанию и формируем выражение select
        selects = []
        errors = ''
        dataframe_dtypes = dict(dataframe.dtypes)
        for col_src, col_new in zip(self.columns, self._get_final_schema()):
            name_src = col_src[0]
            type_src = col_src[1]
            name_new = col_new[0]
            type_new = col_new[1]

            # Проверяем соответствие датафрейма описанию
            if name_src not in dataframe_dtypes.keys():
                errors += f'\nВ датафрейме отсутствует колонка "{name_src}".'
            elif type_src != dataframe_dtypes[name_src]:
                errors += f'\nТип колонки "{name_src}" в описании = "{type_src}", '
                errors += f'а в датафрейме = "{dataframe_dtypes[name_src]}".'

            # Формируем колонку
            select_column = F.col(name_src)
            if self.table_type == 'source':
                if type_src != type_new:
                    select_column = select_column.cast(type_new)
                if name_src != name_new:
                    select_column = select_column.alias(name_new)
            selects.append(select_column)

        if errors:
            raise self.error_message(errors)

        output = dataframe.select(*selects)
        return output

    def load_table(self, spark):
        """Загрузка таблицы из базы"""
        return self.convert_table(spark.table(self.link))

    def get_schema(self) -> StructType:
        """
        Формирование спрак-схемы на базе описания

        Returns
        -------
        schema: pyspark.sql.types.StructType
            схема на основе описания
        """
        schema = StructType([StructField(col[0], TYPES_MAPPING(col[1]), True) for col in self.columns])
        return schema

    def is_subset_of(self, new_description) -> bool:
        """
        Проверка на равенство в смысле "можно ли из текущей таблицы получить новую".
        В данном контексте учитываются только финальные типы.

        Parameters
        ----------
        new_description: TableSignature
            описание новой таблицы
        Returns
        -------
        answer: bool
        """
        if isinstance(new_description, TableSignature):
            final_columns = self._get_final_schema()
            for col in new_description.columns:
                if (col[0], col[1]) not in final_columns:
                    return False
        else:
            raise TypeError(f'Невозможно сравнить тип TableSignature с типом {type(new_description)}.')

        return True

    def recover_link(self, config: ConfigBasePattern):
        """
        Замена алиаса на полный путь к таблицам согласно конфигу

        Parameters
        ----------
        config: ConfigBasePattern
            конфиг проекта
        """
        self.link = config.get_table_link(self.link, True)

    def get_description(self, source_step_name: str = '', indent: int = 0, print_tables: bool = False) -> str:
        """
        Составление описания таблицы

        Parameters
        ----------
        source_step_name: str, optional (default='')
            Название шага, результатом которого является данная таблица
        indent: int, optional (default=0)
            Количество отступов (для корректной вставки в описание)
        print_tables: bool, optional (default=False)
            Печатать ли описание таблицы

        Returns
        -------
        description: str
            описание таблицы
        """
        tab = '    ' * indent
        description = ''

        # Формируем строку с названием шага
        description += tab + f'* {self.table_name}'
        if source_step_name:
            description += f' (Результат вычислений шага {source_step_name})'
        elif self.link:
            description += f' ({self.link})'

        if self.description:
            description += f' ({self.description})'

        # Формируем описание полей таблицы
        if print_tables:
            have_descr = self.table_type == 'pipeline'
            add_col_name = '<th>Описание</th>' if have_descr else ''
            description += ':\n\n' + \
                           tab + '<table>\n' + \
                           tab + '  <thead>\n' + \
                           tab + '  <tr>\n' + \
                           tab + f'    <th>Название колонки</th><th>Формат</th>{add_col_name}\n' + \
                           tab + '  </tr>\n' + \
                           tab + '  </thead>\n' + \
                           tab + '  <tbody>\n'

            for column in self.columns:
                table_row = tab + '  <tr>\n' + \
                            tab +f'    <td>{column[0]}</td><td>{column[1]}</td>'
                if have_descr:
                    if len(column) == 3:
                        table_row += f'<td>{column[2]}</td>'
                    else:
                        table_row += '<td></td>'
                table_row += '\n' + tab + '  </tr>\n'
                description += table_row

            description += tab + '  </tbody>\n' + tab + '</table>\n'

        return description + '\n\n'

# -*- coding: utf-8 -*-
"""
PipelineBase -- для создания последовательности вычислений из объектов типа ШАГ.
-------------------------------------------------------------------------------------
Основной особенностью Pipeline является то, что он принимает на вход только таблицы из базы данных.

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
* проверяется соответствие выходной таблицы пайплайна таблицам, рассчитанным на шагах внутри пайплайна

Пример оформления описания шага:
------------------------------------
>>> '''
>>> Краткое описание модуля в паре слов
>>> --обязательная пустая строка--
>>> Подробное описание модуля (В стиле разметки markdown)
>>> '''
"""
import abc
import logging
import random
from datetime import datetime
from typing import Dict, List, Tuple
from .utils import convert_to_null

LOGGER = logging.getLogger(__name__)


class PipelineBasePattern(abc.ABC):
    """
    Класс для последовательного выполнения шагов.

    output_tables -- словарь таблиц, выдаваемых на выходе из расчёта
    каждая таблица в словаре содержит:
        'link' -- путь к таблице или 'argument', если то таблица передаётся в объект класса
        'description' -- описание таблицы
        'columns' -- информацию о столбцах в виде списка
        [(имя столбца, тип данных, описание колонки (опционально)]
    """
    output_tables = dict()
    step_sequence = []

    def __init__(self, spark, config,
                 step_sequence=None,
                 logger=None,
                 test_arguments=None,
                 skip_structure_check=False,
                 fix_nulls=True
                 ):
        """
        Parameters
        ----------
        spark : запущенный спарк
        config : ConfigBase
            конфиги проекта
        step_sequence : List[StepBase], optional (default=cls.step_sequence)
            список шагов пайплайна
        logger : logger, optional (default=None)
            При None инициализируется свой логгер
        test_arguments: dict, optional (default={})
            словарь таблиц аргументов шагов для тестирования. Передавать только для тестирования!
        skip_structure_check: bool, optional (default=False)
            При тестах может появиться необходимость проверить работу частичного пайплана
            (который будет начинаться с шагов, принимающих таблицы в качестве аргументов).
            Такой пайплайн не пройдёт проверку на структуру. Чтобы её пропустить, задать параметр ``True``.
        fix_nulls: bool, optional (default=True)
            Чинить ли пустые значения выходных таблиц в соответствии с ulits.convert_to_nulls
        """
        self.spark = spark
        self.config = config
        self.fix_nulls = fix_nulls
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
            if 'source_tables' in step.__dict__.keys():
                step_source_tables = step.source_tables
            else:
                step_source_tables = dict()

            step_output_tables = step.output_tables

            # Обновление и проверка на корректность списка HDFS источников шагов пайплайна
            for table_name, table_info in step_source_tables.items():
                if table_info['link'] == 'argument':
                    continue

                if table_name not in pip_source_tables.keys():
                    pip_source_tables[table_name] = {
                        'link': table_info['link'],
                        'columns': [(col_info[0], col_info[1]) for col_info in table_info['columns']]
                    }
                    continue

                # предупреждение, если похожие названия таблиц-источников имеют разные ссылки
                if table_info['link'] != pip_source_tables[table_name]['link']:
                    self.logger.debug(
                        'source table "%s" at step "%s" has link "%s" which is differ from pipeline link "%s"',
                        table_name, step.__name__, table_info['link'], pip_source_tables[table_name]['link']
                    )

                for col_info in table_info['columns']:
                    col, dtype = col_info[0], col_info[1]
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
                for col_info in table_info['columns']:
                    col, dtype = col_info[0], col_info[1]
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

            for el in table_info['columns']:
                col, dtype = el[0], el[1]
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
                    have_descr = max(len(column) for column in table_descr['columns']) == 3
                    description += ':\n\n' + tab + '<table>\n' + tab + '  <thead>\n'
                    if have_descr:
                        description += tab + '  <tr>\n' + tab + \
                                       '    <th>Название колонки</th><th>Формат</th><th>Описание</th>\n' + \
                                       tab + '  </tr>\n'
                    else:
                        description += tab + '  <tr>\n' + tab + \
                                       '    <th>Название колонки</th><th>Формат</th>\n' + tab + '  </tr>\n'

                    description += tab + '  </thead>\n' + tab + '  <tbody>\n'

                    for column in table_descr['columns']:
                        table_row = tab + '  <tr>\n' + tab + '    '
                        table_len = 2 + int(have_descr)
                        for i in range(table_len):
                            if i < len(column):
                                table_row += f'<td>{column[i]}</td>'
                            else:
                                table_row += f'<td></td>'
                        table_row += '\n' + tab + '  </tr>\n'
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
            selected_columns = [col[0] for col in table_descr['columns']]
            # выбор колонок в соответствии с output_tables
            output[table_name] = current_table.select(*selected_columns)

        return convert_to_null(output) if self.fix_nulls else output

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
            if 'source_tables' in step.__dict__.keys():
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
                    raise ValueError(f'Saving error: there is not partition {part} in table {link}. Table columns: {spark_dataframe.columns}')

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
        test_base = self.config.cfg_sources['db_backups']['test'] + '.'
        tmp_table_link = test_base + self.logger.name.replace('.', '_') + \
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
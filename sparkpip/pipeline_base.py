# -*- coding: utf-8 -*-
"""
PipelineBase -- для создания последовательности вычислений из объектов типа ШАГ.
-------------------------------------------------------------------------------------
Основной особенностью Pipeline является то, что он принимает на вход только таблицы из базы данных.

Атрибут класса step_sequence является списком классов (шагов или sql-импортов)
порядок выполнения шагов согласован с последовательностью шагов в step_sequence

Атрибут output_tables аналогичен предыдущим классам

При инициализации:

* проверяется соответствие таблиц-источников и колонок в них на каждом шаге
  с таблицами, рассчитанными на предыдущих шагах
* проверяется будут ли результаты каких-либо шагов перезаписаны на последующих шагах
* собирается граф вычислений в формате nx.DiGraph
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
from datetime import datetime
from typing import Dict, List
import networkx as nx
from .table_description_base import TableDescriptions
from .step_base import SqlOnlyImportBasePattern

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
    step_sequence -- список шагов пайплайна, которые будут выполняться последовательно
    """
    output_tables = {}
    step_sequence = []

    def __init__(self, spark, config,
                 step_sequence=None,
                 logger=None,
                 test_arguments=None,
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

        self._output_tables = TableDescriptions(self.output_tables, 'pipeline', config, test=self.test)

        # Создаём граф вычислений
        self.graph = nx.DiGraph()
        self.build_graph()

        # Список шагов, результаты которых используются в нескольких последующих шагов. Эти таблицы будем кэшировать
        self.cached_steps = []
        self.update_cache()

        # Место для записи результатов вычислений пайплайна
        self.result = {}

    def build_graph(self):
        """
        Формирование графа вычислений пайплайна.

        Граф записывается во внутреннюю переменную `graph`

        Названия нод графа формируются по следующему шаблону:
        * шаг: "step:<название класса шага>"
        * таблица:
          "table:<название класса шага, результатом которого является таблица>:<название таблицы>:<путь к таблице>"
        """
        edges = []
        arguments = {}
        errors = ''
        warns = ''

        # Составляем связи зависимости шагов
        for step in self.step_sequence:
            # Инициализируем шаг, чтобы он проверил корректность таблиц
            step = step(self.spark, self.config, logger=self.logger, skip_loading=True)

            # Настраиваем связи к шагу
            for table_name, table_descr in step._source_tables.to_dict().items():
                step_name = table_descr.step_name
                # По-умолчанию берём данные из текущего шага
                src_step = ''
                src_link = table_descr['link']
                # Если таблица в аргументах, то нужно её найти
                if table_descr['link'] == 'argument':
                    if table_name not in arguments:
                        errors += f'Источник "{table_name}" шага "{step_name}" не был рассчитан на предыдущих шагах.\n'
                        continue

                    src_table = arguments[table_name]
                    # Обновляем данные для ноды
                    src_step = src_table.step_name
                    src_link = src_table['link']
                    # Если нашли, то проверяем на соответствие
                    if not src_table.is_subset_of(table_descr):
                        errors += f'Результат "{table_name}" шага "{src_step}" ' + \
                                  f'не соответствует описанию шага "{step_name}"\n'

                # Добавляем связь
                edges.append((
                    f'table:{src_step}:{table_name}:{src_link}',
                    f'step:{step_name}'))

            # Настраиваем связи из шага
            for table_name, table_descr in step._output_tables.to_dict().items():
                step_name = table_descr.step_name
                table_link = table_descr["link"]
                # Обновляем аргументы
                if table_name in arguments:
                    src_table = arguments[table_name]
                    src_step = src_table.step_name
                    warns += f'Результат "{src_table}" шага "{src_step}" будет перезаписан на шаге "{step_name}".\n'
                arguments[table_name] = table_descr
                # Добавляем связь
                edges.append((
                    f'step:{step_name}',
                    f'table:{step_name}:{table_name}:{table_link}'))

        # Дополняем выходными таблицами
        for table_name, table_descr in self._output_tables.to_dict().items():
            table_link = table_descr["link"]
            src_step = ''
            src_link = table_descr["link"]
            # Проверяем соответствие таблиц
            if table_name in arguments:
                src_table = arguments[table_name]
                src_step = src_table.step_name
                src_link = src_table['link']
                if not src_table.is_subset_of(table_descr):
                    errors += f'Результат "{table_name}" шага "{src_step}" ' + \
                              'не соответствует описанию выхода пайплайна.\n'
            else:
                errors += f'Таблица "{table_name}" пайплайна не была рассчитана на предыдущих шагах.\n'
            # Добавляем связь
            edges.append((
                f'table:{src_step}:{table_name}:{src_link}',
                f'table:pipeline:{table_name}:{table_link}'))

        # Обрабатываем ошибки
        if warns:
            self.logger.warning(warns)

        if errors:
            raise ValueError('В ходе построения пайплайна обнаружены следующие ошибки:\n' + errors)

        # Формируем граф
        self.graph.add_edges_from(edges)

        # Проверяем, что все связи в порядке
        errors = ''
        for node in self.graph.nodes():
            if self.graph.out_degree(node) > 0:
                continue
            _, step_name, table_name, _ = node.split(':')
            if step_name == 'pipeline':
                continue
            errors += f'Таблица "{table_name}" шага "{step_name}" не используется!\n'
        if errors:
            raise ValueError('В ходе построения пайплайна обнаружены следующие ошибки:\n' + errors)

    def get_results_sources(self) -> List[Dict]:
        """
        Поиск источников для результатов расчёта пайплайна

        Returns
        -------
        relations: List[Dict]
            связи результатов с источниками {
                result_table: название таблицы результата,
                result_link: путь к таблице результата,
                source_link: путь к таблице источника,
            }
        """
        # Поиск всех начальных и конечных точек
        start_nodes = []
        end_nodes = []

        for node in self.graph.nodes():
            if self.graph.out_degree(node) == 0:  # Узел не имеет исходящих ребер (детей)
                end_nodes.append(node)
            elif self.graph.in_degree(node) == 0:  # Узел не имеет входящих ребер (родителей)
                start_nodes.append(node)

        # Поиск связей
        relations = []
        for end_node in end_nodes:
            for start_node in start_nodes:
                # Получение всех путей между начальной и конечной точками
                all_paths = nx.all_simple_paths(self.graph, source=start_node, target=end_node)
                if list(all_paths):
                    _, _, _, src_table_link = start_node.split(':')
                    _, _, dst_table_name, dst_table_link = end_node.split(':')
                    relations.append({
                        'result_table': dst_table_name,
                        'result_link': dst_table_link,
                        'source_link': src_table_link
                    })
        return relations

    def update_cache(self):
        """
        Обновление списка кэшируемых шагов на основе графа вычислений.

        Шаги выбираются по принцу: если результат используется более чем в 1 шаге -- кэшируем
        """
        # Собираем список всех шагов, результаты которых должны быть закэшированы
        step_names = []
        for node in self.graph.nodes():
            node_description = node.split(':')
            if node_description[0] == 'step':
                continue
            if self.graph.out_degree(node) < 2:
                continue
            step_names.append(node_description[1])
        step_names = set(step_names)

        # Добавляем шаги в список для кэша
        sql_only_counter = 0
        for step in self.step_sequence:
            step_name = step.__name__
            # Проверяем на необходимость кэширования
            if step_name not in step_names:
                continue
            # Добавляем в список кэша
            step_obj = step(self.spark, self.config, logger=self.logger, skip_loading=True)
            self.cached_steps.append(step)
            # По умолчанию загрузчики не кешируются
            if isinstance(step_obj, SqlOnlyImportBasePattern):
                sql_only_counter += 1
                self.logger.warning(
                    f'Результаты шага "{step_name}" используются в нескольких шагах. ' + \
                    'Но шаг является загрузчиком, поэтому его результаты не кэшируются по-умолчанию. ' + \
                    'Чтобы включить кэширование, используйте .run(not_cache_sql_loaders=False)'
                )
            else:
                self.logger.warning(
                    f'Результаты шага "{step_name}" используются в нескольких шагах и будут закешированы'
                )

        if len(self.cached_steps) > sql_only_counter:
            self.logger.warning(
                """Будьте внимательны с кешированием (лучше не кешировать большие таблицы). 
Параметры кеширования задаются при запуске расчётов `.run()`:
* autocache=False -- отключение функции автокеширования (не отключает кэширование, которое указано в коде расчётов)
* cache_ignore_steps -- список шагов, результаты которых кэшироваться не будут
* not_cache_sql_loaders=True -- отключение кэширования загрузчиков данных (SqlOnlyImportBase)
"""
            )

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

        def print_node(node):
            """Отрисовка ноды"""
            node_description = node.split(':')
            node_name = '_sep_'.join(node_description[:3])
            if node_description[0] == 'step':
                label = f'label="{node_description[1]}"'
                parameters = ', shape=ellipse'
            else:
                if node_description[-1]:
                    label = '<br/>'.join(node_description[-1].split('.'))
                    label = f'label=<{label}>'
                else:
                    label = f'label="{node_description[-2]}"'
                parameters = ''
            return f'    {node_name} [{label}{parameters}]\n'

        def print_edges(node):
            """Отрисовка ребра"""
            node_description = node.split(':')
            node_name = '_sep_'.join(node_description[:3])
            edge = ''
            for src_node in self.graph.pred[node].keys():
                node_description = src_node.split(':')
                src_node_name = '_sep_'.join(node_description[:3])
                edge += f'    {src_node_name} -> {node_name}\n'

            return edge

        for node in self.graph.nodes():
            if self.graph.in_degree(node) == 0:
                sources += print_node(node)
                sources += print_edges(node)
            elif self.graph.out_degree(node) == 0:
                outputs += print_node(node)
                outputs += print_edges(node)
            else:
                calculations += print_node(node)
                calculations += print_edges(node)

        sources += '  }\n'
        calculations += '  }\n'
        outputs += '  }\n'
        graph += '\n'.join([sources, calculations, outputs]) + '}'

        return graph

    def get_pipeline_description(self) -> str:
        """
        Функция вывода описания пайплайна для wiki

        Вывод производится в стиле юпитерского маркдауна потому, что наше вики маркдауна не знает.

        Returns
        -------
        documentation : str
            описание алгоритмов пайплайна
        """
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
        def find_sources(node_name):
            step_sources = {}
            for src_node in self.graph.pred[node_name].keys():
                node_description = src_node.split(':')
                step_sources[node_description[2]] = node_description[1]
            return step_sources

        documentation += '## Модуль состоит из последовательного выполнения следующих шагов:\n'
        for step in self.step_sequence:
            # Инициализируем шаг, чтобы он проверил корректность таблиц
            step = step(self.spark, self.config, logger=self.logger, skip_loading=True)

            # Ищем аргументы
            step_node_name = f'step:{step.__class__.__name__}'
            step_sources = find_sources(step_node_name)

            # Добавляем описание шага
            documentation += step.get_description(step_sources)

        # Добавляем описание выходных таблиц
        documentation += '\n## Результатом выполнения алгоритма являются следующие таблицы:\n'
        step_sources = {}
        for table_name, table_descr in self._output_tables.to_dict().items():
            table_node_name = f'table:pipeline:{table_name}:{table_descr["link"]}'
            step_sources.update(find_sources(table_node_name))
        documentation += self._output_tables.get_description(step_sources)

        return documentation

    def run(self, autocache: bool = True, cache_ignore_steps: List[str] = [],
            not_cache_sql_loaders: bool = True) -> Dict:
        """
        Функция последовательного вычисления шагов пайплайна.

        Parameters
        ----------
        autocache : bool, optional (default=True)
            Использовать ли автоматическое кэширование таблиц пайплайна, которые используются многократно.
        cache_ignore_steps : List[str], optional (default=[])
            Список шагов, для которых автоматическое кэширование не используется.
            Рекомендуется инициализировать класс пайплайна, в логах он напишет, какие шаги закэшировать.
            Затем можно вырать шаги, которые кэшировать не нужно.
        not_cache_sql_loaders : bool, optional (default=True)
            Не кэшировать SQL-импорты. Дань старой версии.

        Returns
        -------
        result : dict
            словарь таблиц с результатами в соотвествии с cls.output_tables
        """
        tables = self.argument_tables
        script_start_time = datetime.now()

        for step in self.step_sequence:
            # инициализируем шаг
            step_obj = step(self.spark, self.config, argument_tables=tables, logger=self.logger, test=self.test)
            # рассчитываем необходимость кэширования
            cache = False
            if (
                    autocache
                    and (step.__name__ in self.cached_steps)
                    and (step.__name__ not in cache_ignore_steps)
                    and (
            not (not_cache_sql_loaders and isinstance(step_obj, SqlOnlyImportBasePattern)))
            ):
                cache = True
            tag = 'Результаты шага будут закешированы!' if cache else ''
            # Начинаем расчёт
            self.logger.debug('Рассчитываем шаг "%s"...' + tag, step.__name__)
            if isinstance(step_obj, SqlOnlyImportBasePattern) and self.test:
                result = {}
            else:
                result = step_obj.run(cached=cache)

            tables.update(result)

        script_end_time = datetime.now()
        self.logger.debug('Pipeline calculations start at %s', script_start_time.strftime('%Y-%m-%d: %H:%M:%S'))
        self.logger.debug('Pipeline calculations end at %s', script_end_time.strftime('%Y-%m-%d: %H:%M:%S'))
        time_delta = (script_end_time - script_start_time).seconds
        hour_delta = time_delta // 3600
        minutes_delta = (time_delta - hour_delta * 3600) // 60
        seconds_delta = (time_delta - hour_delta * 3600) % 60
        self.logger.debug('Whole time %dh %dm %ds', hour_delta, minutes_delta, seconds_delta)

        self.result = self._output_tables.load_tables(self.spark, tables)

        return self.result

    def write_dataframe_hive(self, spark_dataframe, link, mode, partitions=None, parts_n=None,
                             insert=False, disable_repartition=False):
        """
        Функция записи спарковского датафрейма в HIVE

        Parameters
        ----------
        spark_dataframe : spark.DataFrame
            Датафрейм для записи в HIVE
        link : str
            полный путь к таблице
        mode : str
            как записывать таблицу ('overwrite', 'append')
        partitions : list, optional (default=None)
            список полей, по которым будет производиться партиционирование
        parts_n : int, optional (default=None)
            количество партиций при записи
            None -- дефолтное количество, определяемое при инициализации спарка
        insert: bool
            использовать мод .insertInto(mode=mode)
        disable_repartition: bool, optional (default=False)
            отключение репартиционирования таблицы перед записью.
        """
        # Проверка партиций на адекватность
        if partitions is not None:
            for part in partitions:
                if part not in spark_dataframe.columns:
                    raise ValueError(
                        f'Ошибка при сохранении таблицы: Партиция "{part}" таблицы "{link}" отсутствует в данных.' + \
                        f'Список колонок таблицы: {spark_dataframe.columns}')

        # Создание таблицы в случае если она не существует, но потребуется
        if (not self.spark.catalog._jcatalog.tableExists(link)) and insert:
            LOGGER.warning(f'Таблица "{link}" отсутствует в БД. Начинаю создание таблицы...')
            sdf = self.spark.createDataFrame([], schema=spark_dataframe.schema)
            if partitions is not None:
                sdf.write.saveAsTable(link, mode='overwrite', partitionBy=partitions)
            else:
                sdf.write.saveAsTable(link, mode='overwrite')

        # В случае дополнения таблицы лучше восстановить порядок колонок
        if insert:
            init_columns = spark_dataframe.columns
            table_columns = self.spark.table(link).columns
            columns_diff = set(init_columns).symmetric_difference(set(table_columns))
            if columns_diff:
                raise ValueError(
                    f'Не могу записать таблицу "{link}" в моде insert из-за несоответствия колонок.' + \
                    f'Список не совпадающих колонок: {columns_diff}'
                )
            spark_dataframe = spark_dataframe.select(table_columns)

        # Репартиционирование таблицы
        if not disable_repartition:
            if partitions is not None:
                if parts_n is not None:
                    spark_dataframe = spark_dataframe.repartition(parts_n, partitions)
                else:
                    spark_dataframe = spark_dataframe.repartition(*partitions)
            elif parts_n is not None:
                spark_dataframe = spark_dataframe.repartition(parts_n)

        # Запись таблицы
        self.logger.debug(f'Начинаю сохранение таблицы "{link}". mode={mode}, partititons={partitions}...')
        writer = spark_dataframe.write
        if insert:
            is_overwrite = mode == 'overwrite'
            writer.insertInto(link, overwrite=is_overwrite)
        else:
            writer = writer.mode(mode)
            if partitions is not None:
                writer = writer.partitionBy(partitions)
            writer.saveAsTable(link)

    def write_dataframe_hive_over_tmp(self, table_name, link, mode, partitions=None, parts_n=None,
                                      insert=False, update_parameters={}):
        """
        Функция записи спарковского датафрейма в HIVE через временную таблицу:

        * датафрейм ``self.result[table_name]`` записывается во временную таблицу
        * датафрейм ``self.result[table_name]`` удаляется и создаётся датафрейм из временной таблицы
        * датафрейм из временной таблицы записывается по адресу ``link`` и удаляется

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
            По-умолчанию партиции отсутствуют.
        parts_n : int, optional (default=None)
            количество партиций при записи
            По-умолчанию -- дефолтное количество, определяемое при инициализации спарка
        insert: bool
            использовать мод .insertInto(mode=mode)
        update_parameters: Dict, optional (default={})
            `keys` -- список ключей по которым нужно произвести обновление
            `filter` -- дополнительный фильтр на исходную таблицу
        """
        # Генерируем имя временной таблицы так, чтобы оно не совпадало с уже существующей таблицей
        test_base = self.config.cfg_sources['db_backups']['test'] + '.'
        tag = f"_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        tmp_table_link = test_base + link.split('.')[1] + '_tmp_table' + tag
        # На случай, если мод обновления update нужно будет куда-то записывать старые данные
        tmp_table_link_add = test_base + link.split('.')[1] + '_tmp_table_add' + tag

        # Записываем результат во временную таблицу
        self.logger.debug(f'Сохраняю результат во временную таблицу {tmp_table_link}...')
        self.write_dataframe_hive(self.result[table_name], tmp_table_link, 'overwrite', partitions, parts_n)

        # В случае update мода докидываем данные предыдущего результата с исключением новых данных
        if mode == 'update':
            if 'keys' not in update_parameters:
                raise ValueError(
                    'При записи в режиме update необходимо задать список ключей для обновления ' + \
                    '(update_parameters["keys"] = [col1, col2...])'
                )
            src_table = self.spark.table(link)
            if 'filter' in update_parameters:
                src_table = src_table.filter(update_parameters['filter'])

            old_data = (
                src_table.join(
                    self.spark.table(tmp_table_link).select(update_parameters['keys']),
                    on=update_parameters['keys'],
                    how='left_anti'
                )
            )
            self.logger.debug(f'Сохраняю старые данные во временную таблицу {tmp_table_link_add}...')
            self.write_dataframe_hive(old_data, tmp_table_link_add, 'overwrite', partitions, parts_n)

        # Формируем результат
        if mode == 'update':
            columns = self.spark.table(link).columns
            output = (
                self.spark.table(tmp_table_link).select(columns)
                .union(self.spark.table(tmp_table_link_add).select(columns))
            )
        else:
            output = self.spark.table(tmp_table_link)

        # Записываем данные из временной таблицы в link.
        try:
            if mode == 'update':
                self.write_dataframe_hive(output, link, 'overwrite', partitions, parts_n, insert=True)
            else:
                self.write_dataframe_hive(output, link, mode, partitions, parts_n, insert=insert)
            self.logger.debug('Процесс записи завешён, удаляю временные таблицы...')
        except Exception as exception:
            # Удаляем временные таблицы в случаем неудачной записи.
            self.logger.error(f'Не получилось записать таблицу "{link}"! Удаляю временные таблицы...')
            raise exception
        finally:
            del self.result[table_name]
            sql = 'DROP TABLE IF EXISTS ' + tmp_table_link
            self.logger.debug(sql)
            self.spark.sql(sql)
            sql = 'DROP TABLE IF EXISTS ' + tmp_table_link_add
            self.logger.debug(sql)
            self.spark.sql(sql)

    def save_result_to_hive(self, table_name='all', num_partitions=None, partitions=None,
                            save_mode='append', insert=False, table_link=None, use_tmp_table=False,
                            update_parameters={}, disable_repartition=False):
        """
        Модуль сохранение результатов вычислений пайплайна в HIVE

        Parameters
        ----------
        table_name : str, optional (default='all')
            имя таблицы из результатов пайплайна, которую нужно записать в HIVE
            'all' -- в случае записи всех таблиц, указанных в ``output_tables``
        num_partitions : int, optional (default=None)
            количество партиций при записи
            По-умолчанию -- дефолтное количество из конфига (config_sources)
            В случае отсутствия конфига берётся `spark.sql.shuffle.partitions`
        partitions : list, optional (default=None)
            список полей, по которым будет производиться партиционирование
            По-умолчанию -- партиции из конфига (config_sources)
        save_mode : str, optional (default='append')
            как записывать таблицу:
            * 'overwrite' -- перезапись
            * 'append' -- добавление в существующую таблицу. При отсутствие таблицы работает, как 'overwrite'
            * 'update' -- обновление данных в таблице по ключам update_parameters['keys'].
              Работать будет долго, так как это не дефолтный метод спарка.
        insert: bool
            использовать мод .insertInto(mode=mode)
        table_link : str, optional (default=None)
            полный путь к таблице или имя таблицы в соответствии с config_sources.
            По-умолчанию -- путь будет браться из описания ``output_tables``
        use_tmp_table: bool
            Запись через временную таблицу.
            По-умолчанию запись через временную таблицу используется при попытке записи в таблицу,
            которая используется в качестве источника расчёта.
        update_parameters: Dict, optional (default={})
            `keys` -- список ключей по которым нужно произвести обновление
            `filter` -- дополнительный фильтр на исходную таблицу
        disable_repartition: bool, optional (default=False)
            отключение репартиционирования таблицы перед записью.
            Лучше не использовать без ясного понимания, что перед записью датафрейм партиционирован нормально
        """
        # Засекаем время
        saving_start_time = datetime.now()
        self.logger.debug('Saving %s table from pipeline results start at %s',
                          table_name,
                          saving_start_time.strftime('%Y-%m-%d: %H:%M:%S'))

        # Проверка режима записи
        available_modes = ['overwrite', 'append', 'update']
        if save_mode not in available_modes:
            raise ValueError(
                f'Недопустимое значение параметра save_mode "{save_mode}". Возможные значения: {available_modes}'
            )

        # Проверка наличия необходимых параметров режима update
        if save_mode == 'update':
            if 'keys' not in update_parameters:
                raise ValueError(
                    'При записи в режиме update необходимо задать список ключей для обновления ' + \
                    '(update_parameters["keys"] = [col1, col2...])'
                )
            if 'filter' not in update_parameters:
                self.logger.warning(
                    'Для ускорения записи в режиме update лучше задать параметры фильтрации данных.' + \
                    'В противном случае обновление потребует перезаписи всей целевой таблицы.'
                )

        # Формируем список таблиц для записи
        if table_name == 'all':
            tables_to_write = self._output_tables.to_dict()
        else:
            tables_to_write = {
                table_name: self._output_tables.to_dict().get(table_name, None)
            }
            if tables_to_write[table_name] is None:
                raise ValueError(f'Таблица "{table_name}" отсутствует в описании выходных таблиц пайплайна')

        # Проверка на конфликт параметров при нескольких таблицах
        if len(tables_to_write.keys()) != 1:
            if (
                (num_partitions is not None)
                or (partitions is not None)
                or (save_mode == 'update')
                or (table_link is not None)
                or disable_repartition
            ):
                raise KeyError(
                    'Параметры "num_partitions", "partitions", "table_link", "save_mode", "disable_repartition" ' + \
                    'не поддерживаются в режиме записи всех таблиц-результатов пайплайна (table_name == "all").'
                )

        # Начинаем процесс записи
        for tbl_name, table_description in tables_to_write.items():
            default_link = self.config.get_table_link(table_description['link'], quiet_mode=True)

            # Формируем адрес для записи таблицы
            if table_link is not None:
                table_link = self.config.get_table_link(table_link, quiet_mode=True)
                if table_link != default_link:
                    self.logger.warning(
                        'Путь для записи таблицы был задан отличным от конфига! ' + \
                        f'Таблица будет записана по адресу {table_link}.'
                    )
            else:
                table_link = self.config.get_table_link(table_description['link'], quiet_mode=True)

            # Задаём параметры партиционирования
            if (partitions is None) and (num_partitions is None):
                _, _, default_parts, _ = self.config.get_table_description(table_link)
                if default_parts:
                    if isinstance(default_parts[0], str):
                        partitions = default_parts
                    if isinstance(default_parts[0], int):
                        num_partitions = default_parts[0]

            # Определяем необходимость записи через временную таблицу
            if not use_tmp_table:
                relations = self.get_results_sources()
                for rel in relations:
                    if (rel['result_table'] == tbl_name) and (rel['result_link'] == rel['source_link']):
                        use_tmp_table = True
                        self.logger.warning(
                            f'Таблица {tbl_name} будет записана в {table_link} через временную, ' + \
                            'так как использует себя в качестве источника расчёта.'
                        )
                        break

            # Запись
            if use_tmp_table or (save_mode == 'update'):
                if disable_repartition:
                    self.logger.warning(
                        'В случае записи результата через временную таблицу режим disable_repartition не работает.'
                    )
                self.write_dataframe_hive_over_tmp(
                    tbl_name, table_link, save_mode,
                    partitions=partitions,
                    parts_n=num_partitions,
                    insert=insert,
                    update_parameters=update_parameters
                )
            else:
                self.write_dataframe_hive(
                    self.result[tbl_name], table_link, save_mode,
                    partitions=partitions,
                    parts_n=num_partitions,
                    insert=insert,
                    disable_repartition=disable_repartition
                )

            # Если нужно записать более 1 таблицы, обновляем параметры
            num_partitions = None
            partitions = None
            table_link = None

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

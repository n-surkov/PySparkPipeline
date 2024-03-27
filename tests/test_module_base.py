#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
python3 -m unittest test/final_monitoring.py

"""
import unittest
import os
import numpy as np
import logging
from datetime import datetime
from pyspark.sql.functions import col as spark_col
from pyspark.sql import SparkSession, SQLContext
import sparkpip

DIRNAME = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(DIRNAME, 'config.yml')
CFG_SOURCES_PATH = os.path.join(DIRNAME, 'config_sources.yml')

LOGGER = logging.getLogger(__name__)


class ModuleBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.config("spark.sql.shuffle.partitions", "8")
            .master("local")
            .getOrCreate()
        )
        cls.sc = cls.spark.sparkContext
        cls.sql_spark = cls.spark.sql
        cls.sqlContext = SQLContext(cls.sc)
        cls.config = sparkpip.ConfigBasePattern(CFG_PATH, CFG_SOURCES_PATH)
        
        columns = ['plu', 'date', 'week', 'price', 'qty']
        values = [
            ('555', '2020-01-01', 1, 81.99, 5.),
            ('555', '2020-01-02', 1, 85.99, 2.),
            ('555', None, 1, np.nan, 9.),
            ('', '2020-01-04', 1, 79.99, 9.),
        ]
        cls.table = cls.spark.createDataFrame(values, columns)
        
        class FirstStep(sparkpip.StepBasePattern):
            source_tables = {
                'input_table': {
                    'link': 'link_for_input_table',
                    'description': None,
                    'columns': [
                        ('plu', 'string', 'plu_code', None),
                        ('date', 'string', 'date', 'date'),
                        ('week', 'bigint', 'week', 'int'),
                        ('price', 'double', 'price', None),
                        ('qty', 'double', 'qty', None)
                    ]
                }
            }
            output_tables = {
                'interm_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('week', 'int'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            def _calculations(self):
                output = self.tables['input_table'].filter(spark_col('plu_code').isNotNull())

                return {'interm_table': output}
            
        cls.first_step_class = FirstStep

        class SecondStep(sparkpip.StepBasePattern):
            source_tables = {
                'interm_table': {
                    'link': 'argument',
                    'description': None,
                    'columns': [
                        ('plu_code', 'string', 'plu_code', None),
                        ('date', 'date', 'date', None),
                        ('week', 'int', 'week', None),
                        ('price', 'double', 'price', None),
                        ('qty', 'double', 'qty', None)
                    ]
                }
            }
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('week', 'int'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            def _calculations(self):
                output = self.tables['interm_table'].filter(spark_col('price').isNotNull())

                return {'output_table': output}

        cls.second_step_class = SecondStep

    @classmethod
    def tearDownClass(cls):
        if 'sc' in globals():
            cls.sc.stop()

    def test_config_set_item(self):
        # проверка на присваивание
        self.config['tmp_parameter'] = 'test'
        self.assertEqual(self.config.tmp['tmp_parameter'], self.config['tmp_parameter'])
        self.assertEqual(self.config['tmp_parameter'], 'test')

        try:
            self.config['TEST'] = True
            is_update = True
        except KeyError as msg:
            is_update = False
            self.assertEqual(msg.args[0], 'Параметр "TEST" задан в качестве параметра конфига. Для перезаписи используйте метод update_config.')
        self.assertFalse(is_update, 'Не должны обновляться параметры конфига через setitem')

    def test_config_update_config(self):
        # обновление конфига
        old_test_value = self.config['TEST']
        new_cfg = {'TEST': ~old_test_value}
        self.config.update_config(new_cfg)
        self.assertEqual(self.config['tmp_parameter'], 'test')

    def test_config_get_table_link(self):
        # Ссылки на таблицы
        new_cfg = {'TEST': False}
        self.config.update_config(new_cfg)
        dict_plu = self.config.get_table_link('goods_tbl')
        self.assertEqual('edw_db.goods_table', dict_plu)
        monitoring_predicted_tbl = self.config.get_table_link('product_table_1')
        self.assertEqual('product_prod_db.table_name_in_database', monitoring_predicted_tbl)
        notable = self.config.get_table_link('notable', True)
        self.assertEqual(notable, 'notable')
        try:
            notable = self.config.get_table_link('notable')
            self.assertEqual(1, 0, 'Не должна даваться ссылка на несуществующую таблицу')
        except KeyError as msg:
            pass

    def test_config_get_date(self):
        # Смотрим на дату
        pattern = '%Y-%m-%d'
        current_date = datetime.today().date().strftime(pattern)
        today = self.config.get_date(pattern)
        yesterday = self.config.get_date(pattern, 1)
        self.assertEqual(today, current_date)
        self.assertNotEqual(yesterday, current_date)

    def test_correct_step(self):
        # Проверяем корректность отработки шага
        step = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                     test=True, logger=LOGGER)
        res = step.run()
        cnt = res['interm_table'].count()
        self.assertEqual(cnt, 3)

    def test_step_input_table_mismatch(self):
        try:
            step = self.first_step_class(self.spark, self.config, {'some_table': self.table},
                                         test=True, logger=LOGGER)
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'Таблица "input_table" шага "FirstStep" согласно описанию должна передаваться, как аргумент, но отсутствует в аргументах.')
#
    def test_step_input_type_mismatch(self):
        old_col = self.first_step_class.source_tables['input_table']['columns'][0]
        self.first_step_class.source_tables['input_table']['columns'][0] = ('plu', 'double')
        try:
            step = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                         test=True, logger=LOGGER)
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             """Ошибка в описании таблицы "input_table" шага "FirstStep":
Тип колонки "plu" в описании = "double", а в датафрейме = "string".""")
        finally:
            self.first_step_class.source_tables['input_table']['columns'][0] = old_col

    def test_step_input_column_mismatch(self):
        self.first_step_class.source_tables['input_table']['columns'].append(('nocol', 'string'))
        try:
            step = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                         test=True, logger=LOGGER)
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             """Ошибка в описании таблицы "input_table" шага "FirstStep":
В датафрейме отсутствует колонка "nocol".""")
        finally:
            old_cols = self.first_step_class.source_tables['input_table']['columns'][:-1]
            self.first_step_class.source_tables['input_table']['columns'] = old_cols

    def test_step_output_table_mismatch(self):
        self.first_step_class.output_tables['out_table'] = self.first_step_class.output_tables['interm_table']
        try:
            _ = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                      test=True, logger=LOGGER).run()
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'Таблица "out_table" шага "FirstStep" согласно описанию должна присутствовать в результатах расчёта, но отсутствует.')

    def test_step_output_type_mismatch(self):
        old_col = self.first_step_class.output_tables['interm_table']['columns'][0]
        self.first_step_class.output_tables['interm_table']['columns'][0] = ('plu_code', 'double')
        try:
            _ = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                      test=True, logger=LOGGER).run()
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             """Ошибка в описании таблицы "interm_table" шага "FirstStep":
Тип колонки "plu_code" в описании = "double", а в датафрейме = "string".""")
        finally:
            self.first_step_class.output_tables['interm_table']['columns'][0] = old_col

    def test_step_output_column_mismatch(self):
        self.first_step_class.output_tables['interm_table']['columns'].append(('nocol', 'string'))
        try:
            _ = self.first_step_class(self.spark, self.config, {'input_table': self.table},
                                      test=True, logger=LOGGER).run()
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             """Ошибка в описании таблицы "interm_table" шага "FirstStep":
В датафрейме отсутствует колонка "nocol".""")
        finally:
            old_cols = self.first_step_class.output_tables['interm_table']['columns'][:-1]
            self.first_step_class.output_tables['interm_table']['columns'] = old_cols

    def test_correct_pipeline(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        df_dtypes = [
            ('plu_code', 'string'),
            ('date', 'date'),
            ('price', 'double'),
            ('qty', 'double')
        ]

        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': df_dtypes
                }
            }

            step_sequence = [self.first_step_class, self.second_step_class]

        pip = Pipeline(self.spark, self.config, logger=LOGGER, test_arguments={'input_table': self.table})
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        result = pip.run()
        output_table = result['output_table']
        self.assertEqual(list(result.keys()), ['output_table'])
        self.assertEqual(output_table.count(), 2)
        self.assertEqual(df_dtypes, output_table.dtypes)

    def test_pipeline_wrong_step_argument(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            step_sequence = [self.second_step_class]

        try:
            pip = Pipeline(self.spark, self.config, logger=LOGGER)
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'В ходе построения пайплайна обнаружены следующие ошибки:\n' + \
                             'Источник "interm_table" шага "SecondStep" не был рассчитан на предыдущих шагах.\n')

    def test_pipeline_wrong_interm_column(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        self.second_step_class.source_tables['interm_table']['columns'].append(('nocol', 'string', 'nocol', None))
        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            step_sequence = [self.first_step_class, self.second_step_class]

        try:
            pip = Pipeline(self.spark, self.config, logger=LOGGER, test_arguments={'input_table': self.table})
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'В ходе построения пайплайна обнаружены следующие ошибки:\n' + \
                             'Результат "interm_table" шага "FirstStep" не соответствует описанию шага "SecondStep"\n')
        finally:
            old_cols = self.second_step_class.source_tables['interm_table']['columns'][:-1]
            self.second_step_class.source_tables['interm_table']['columns'] = old_cols

    def test_pipeline_wrong_interm_column_type(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        old_col = self.second_step_class.source_tables['interm_table']['columns'][0]
        self.second_step_class.source_tables['interm_table']['columns'][0] = ('plu_code', 'double', 'plu_code', None)
        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            step_sequence = [self.first_step_class, self.second_step_class]

        try:
            pip = Pipeline(self.spark, self.config, logger=LOGGER, test_arguments={'input_table': self.table})
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'В ходе построения пайплайна обнаружены следующие ошибки:\n' + \
                             'Результат "interm_table" шага "FirstStep" не соответствует описанию шага "SecondStep"\n')
        finally:
            self.second_step_class.source_tables['interm_table']['columns'][0] = old_col

    def test_pipeline_wrong_output_column(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'string'),
                        ('date', 'date'),
                        ('price', 'double'),
                        ('qty', 'double'),
                        ('nocol', 'string')
                    ]
                }
            }

            step_sequence = [self.first_step_class, self.second_step_class]

        try:
            pip = Pipeline(self.spark, self.config, logger=LOGGER, test_arguments={'input_table': self.table})
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'В ходе построения пайплайна обнаружены следующие ошибки:\n' + \
                             'Результат "output_table" шага "SecondStep" не соответствует описанию выхода пайплайна.\n')
        finally:
            old_cols = self.second_step_class.source_tables['interm_table']['columns'][:-1]
            self.second_step_class.source_tables['interm_table']['columns'] = old_cols

    def test_pipeline_wrong_output_column_type(self):
        self.first_step_class.source_tables['input_table']['link'] = 'link_for_input_table'
        class Pipeline(sparkpip.PipelineBasePattern):
            output_tables = {
                'output_table': {
                    'link': None,
                    'description': None,
                    'columns': [
                        ('plu_code', 'double'),
                        ('date', 'date'),
                        ('price', 'double'),
                        ('qty', 'double')
                    ]
                }
            }

            step_sequence = [self.first_step_class, self.second_step_class]

        try:
            pip = Pipeline(self.spark, self.config, logger=LOGGER, test_arguments={'input_table': self.table})
            self.assertEqual(1, 0, 'Шаг не должен выполняться!')
        except Exception as msg:
            self.assertEqual(msg.args[0],
                             'В ходе построения пайплайна обнаружены следующие ошибки:\n' +
                             'Результат "output_table" шага "SecondStep" не соответствует описанию выхода пайплайна.\n')
        finally:
            old_cols = self.second_step_class.source_tables['interm_table']['columns'][:-1]
            self.second_step_class.source_tables['interm_table']['columns'] = old_cols


if __name__ == '__main__':
    unittest.main()

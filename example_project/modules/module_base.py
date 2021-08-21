# -*- coding: utf-8 -*-
"""
Задание классов шагов и пайплайнов, адаптированноых под конкретный проект

В данном примере ничего нового в классы не добавлялось
"""
import abc
from typing import Dict

import sparkpip


class StepBase(sparkpip.StepBasePattern):
    """
    Класс типа ШАГ, адаптированный под конкретный проект
    """
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
        super().__init__(spark, config, argument_tables, test, logger)

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


class PipelineBase(sparkpip.PipelineBasePattern):
    """
    Класс типа ШАГ, адаптированный под конкретный проект
    """

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
        super().__init__(spark, config, step_sequence, logger,
                         test_arguments, skip_structure_check, fix_nulls)
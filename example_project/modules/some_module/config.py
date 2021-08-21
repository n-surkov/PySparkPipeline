# -*- coding: utf-8 -*-
"""
Кнфигурация модуля
"""
from modules.config_base import ConfigBase


CONFIG = ConfigBase()

CONFIG.add_parameter("validateAS", '--vas', type=int, required=False,
                     default=0,
                     help="""Валидировать ли вычисления шага AnotherStep"""
                     )

# -*- coding: utf-8 -*-
"""
Задание класса конфига, адаптированного под конкретный проект
"""
import os
import sys
from datetime import datetime
import logging

# Импорт конфигов
MODULES_BASE_PATH = os.path.dirname(__file__)
sys.path.append(os.path.join(MODULES_BASE_PATH, '..', '..'))
import sparkpip
CFG_PATH = os.path.join(MODULES_BASE_PATH, '..', 'config.yml')
CFG_SOURCES = os.path.join(MODULES_BASE_PATH, '..', 'config_sources.yml')


class ConfigBase(sparkpip.ConfigBasePattern):
    """
    Класс конфига, адаптированный под конкретный проект
    """
    def __init__(self):
        super(ConfigBase, self).__init__(CFG_PATH, CFG_SOURCES, calc_date_format='%d.%m.%Y')
        self.add_parameter('logging_level', '--logging_level', '-ll',
                           type=int, required=False, const=10, default=10, nargs='?',
                           help='10 - ERROR logging_level, 20 - info logging level')

    def tune_logger(self, logger, date=None, log_prefix=None):
        """
        Инициализация параметров логгера
        Parameters
        ----------
        logger : логгер
        date : str, optional (default=today)
         дата логов
        log_prefix : str, optional (default=__name__)
         префикс логов
        """

        if date is None:
            date = datetime.today().date().strftime("%Y-%m-%d")

        if log_prefix is None:
            log_prefix = logger.name

        log_file_name = 'log_{}_{}.log'.format(log_prefix, date)
        log_file_path = os.path.join(MODULES_BASE_PATH, '..', 'data', 'logs', log_file_name)
        logging.basicConfig(level=self.parameters['logging_level'],
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            filename=log_file_path,
                            filemode='w')
        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(self.parameters['logging_level'])
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)

        # Now, we can log to the root logger, or any other logger. First the root...
        logging.info('initial log row')
        # отключим массивные уведомления от spark
        logging.getLogger("py4j").setLevel(logging.ERROR)
        # отключим массивные уведомления от matplotlib
        logging.getLogger("matplotlib").setLevel(logging.ERROR)

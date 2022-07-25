# -*- coding: utf-8 -*-
"""
Здесь содержится класс для работы с конфигами проекта.

Класс работает с двумя типами конфигураций:
* статические
* динамические

Статические конфигурации загружаются из yaml-файлов (пример содержится в test_project):
* config.yml -- конфигурации проекта
* config_sources.yml -- конфиг с путями к таблицам, использующимся в проекте

Динамические конфиги передаются в качестве аргументов при запуске скрипта и считываются при помощи argparse
"""

import argparse
from datetime import datetime, timedelta
from .utils import config_loader


class ConfigBasePattern:
    """
    Класс для работы с параметрами настройки в продукте ценообразования

    Внутренние переменные (в порядке доступа):
    parameters -- параметры, полученные из argparse
    cfg -- импортированные config.yml
    cfg_sources -- импортированные config_sources.yml
    tmp -- временные параметры
    """
    # шаблон хранения описания параметра в классе
    parameter_pattern = {
        'short_flag': None,
        'flag': None,
        'argparse_arguments': None,
    }

    # список аргуметов argparse на проверку
    argparse_arguments = ['action', 'nargs', 'const', 'default', 'type',
                          'choices', 'required', 'help', 'metavar']

    def __init__(self, cfg_path, cfg_sources_path, calc_date_format='%d.%m.%Y'):
        """
        Инициализируются параметры класса:
        cfg -- чтением из файла config.yml
        cfg_sources -- чтением из файла config_sources.yml
        tmp -- пустым словарём
        parameters -- пустым словарём
        parameters_for_parsing -- пустым словарём

        Затем в parameters_for_parsing при помощи функции add_parameter добавляются обязательные параметры:
        * TEST -- для определение в какой базе (тестовой или продовой) будет проводиться расчёт
        * calc_date -- для определения даты вычислений

        Parameters
        ----------
        cfg_path: str
            путь к файлу config.yml с базовыми настройками
        cfg_sources_path: str
            путь к файлу config_sources.yml с описанием таблиц, использующихся в проекте
        calc_date_format: str, optional (default='%d.%m.%Y')
            формат, в котором будет передаваться ата вычислений в качестве аргумента
        """
        # Основные конфиги проекта
        self.cfg = config_loader(cfg_path)
        # Источники таблиц
        self.cfg_sources = config_loader(cfg_sources_path)
        # Вот захотелось тебе добавить параметр и вот сюда его
        self.tmp = dict()
        # Параметры, которые будут использоваться при парсинге
        self.parameters_for_parsing = dict()
        # Параметры, которые получены парсингом.
        self.parameters = dict()
        # Параметры, которые будут использоваться при парсинге
        self.add_parameter('TEST', '--TEST', type=self.str2bool, const=True, default=False, nargs='?',
                           help='True if calculate on test database')
        dt_example = datetime.today().strftime(calc_date_format)
        self.add_parameter('calc_date', '--calc_date', '-dt',
                           type=str, required=False, default=datetime.today().strftime(calc_date_format), nargs='?',
                           help=f'Current date for algorithms in format {dt_example}')
        # Формат даты расчёта
        self.calc_date_format = calc_date_format

    def __setitem__(self, key, value):
        """
        Добавление параметра в конфиг.

        Если конфиг с таким именем уже существует в статических или динамических, то присвоения не произойдёт,
        в ином случае создастся или заменится значение конфига в атрибуте tmp.
        """
        if key in self.cfg.keys():
            raise KeyError('parameter "{}" already exists in config file. Use update_config to overwrite.'.format(key))
        
        if key in self.cfg_sources.keys():
            raise KeyError('parameter "{}" already exists in config_sources file. Use update_config to overwrite.'.format(key))
        
        if key in self.parameters.keys():
            raise KeyError('parameter "{}" already exists in parameters for command line parsing. Use update_config to overwrite.'.format(key))
        
        self.tmp[key] = value

    def update_config(self, new_configs):
        """
        Функция обновления параметров, записанных в конфиге.

        Используется если нужно перезаписать параметры, импортированные в конфиг из файлов и командной строки
        Если в новых конфигах содержатся поля из атрибутов cfg, cfg_sources или parameters, то значение
        этих полей обновляются. Если поле не содержится в конфигах, то оно создаётся в атрибуте tmp.

        Parameters
        ----------
        new_configs: dict
            словарь новых значений параметров в виде {имя паарметра: новое значение}
        """
        for key, val in new_configs.items():
            if key in self.cfg.keys():
                self.cfg[key] = val
                continue

            if key in self.cfg_sources.keys():
                self.cfg_sources[key] = val
                continue

            if key in self.parameters.keys():
                self.parameters[key] = val
                continue

            self.tmp[key] = val

    def __getitem__(self, item):
        """
        Выдаёт значение конфига в порядке приоритета: parameters->cfg->cfg_sources->tmp

        Parameters
        ----------
        item: key

        Returns
        -------
        значение запрашиваемого конфига
        """
        if item in self.parameters.keys():
            return self.parameters[item]

        if item in self.cfg.keys():
            return self.cfg[item]

        if item in self.cfg_sources.keys():
            return self.cfg_sources[item]

        if item in self.tmp.keys():
            return self.tmp[item]

        raise KeyError('there is no variable "{}" in cfg, cfg_sources, parameters and tmp'.format(item))

    def set_default(self, parameter_name='all'):
        """
        Инициализация parameters дефолтными значениями, или None, если не задано значения параметра по дефолту.
        Parameters
        ----------
        parameter_name : str, optional (default='all')
            имя параметра, значение которого нужно сделать дефолтным
            в случае 'all' дефолтными будут инициализированы все параметры
        """
        if parameter_name == 'all':
            parameters_names = self.parameters_for_parsing.keys()
        else:
            if parameter_name not in self.parameters_for_parsing.keys():
                raise KeyError('Config has not parameter {} for parsing'.format(parameter_name))
            parameters_names = [parameter_name]

        for name in parameters_names:
            descr = self.parameters_for_parsing[name]
            if 'default' in descr['argparse_arguments'].keys():
                self.parameters[name] = descr['argparse_arguments']['default']
            else:
                self.parameters[name] = None

    def print_description(self):
        """
        Вывод списков настроек и их значений, содержащихся в конфиге, в формате yml.
        """
        separator = "---------------------------------------------------"

        def _print_info(info):
            print(separator)
            print(info)
            print(separator)

        def _dict_for_printing(dictionary, tab):
            string = ''
            for key, val in dictionary.items():
                if isinstance(val, dict):
                    string += '\n' + tab + key + ' : ' + _dict_for_printing(val, tab + '\t')
                else:
                    string += '\n' + tab + key + ' : ' + '{}'.format(val)
            return string

        def _print_block(info, dictionary):
            print(separator)
            print(info)
            print(_dict_for_printing(dictionary, ''))
            print(separator)

        if len(self.cfg) == 0:
            _print_info('global_config is empty')
        else:
            _print_block('global_config contains:', self.cfg)

        if len(self.cfg_sources) == 0:
            _print_info('sources_config is empty')
        else:
            _print_block('sources_config contains:', self.cfg_sources)

        if len(self.parameters) == 0:
            _print_info('trhere is no parameters in Config')
        else:
            _print_block('parameters contain:', self.parameters)

        if len(self.tmp) == 0:
            _print_info('There is no temporary data in Config')
        else:
            _print_block('temporary data contains:', self.tmp)

    def add_parameter(self, name, flag, short_flag=None, **argparse_arguments):
        """
        Функция добавления параметра в словарь парметров класса.

        Parameters
        ----------
        name : str
            имя параметра (это имя будет использоваться в качестве dest при парсинге)
        flag : str
            имя параметра в командной строке
        short_flag : str,  optional (default=None)
            сокращённое имя параметра
        argparse_arguments : {**kwargs}
            аргументы для парсинга параметра из командной строки
        """
        if name in self.parameters_for_parsing.keys():
            raise KeyError('parameter {} already exists'.format(name))

        for key in argparse_arguments:
            if key not in self.argparse_arguments:
                raise KeyError("argument {} doesn't exist in argparse".format(key))

        self.parameters_for_parsing[name] = {'short_flag': short_flag,
                                             'flag': flag,
                                             'argparse_arguments': argparse_arguments}

        self.set_default(name)

    def parse_arguments(self, arg_list=None):
        """
        Функция парсинга конфигов из командной строки в переменную класса parameters.

        Parameters
        ----------
        arg_list : list of str, optional (default=None)
            список параметров из словаря класса для парсинга.
            По-умолчанию парсятся все параметры, заданные в классе.
        """
        if arg_list is not None:
            parameters_for_parsing = {key: self.parameters_for_parsing[key] for key in arg_list}
        else:
            parameters_for_parsing = self.parameters_for_parsing

        parser = argparse.ArgumentParser(description='list of parameters')

        for name, descr in parameters_for_parsing.items():
            if descr['short_flag'] is not None:
                parser.add_argument(descr['short_flag'], descr['flag'], dest=name, **descr['argparse_arguments'])
            else:
                parser.add_argument(descr['flag'], dest=name, **descr['argparse_arguments'])

        self.parameters = vars(parser.parse_args())

    # Далее общие полезные функции
    def tune_logger(self, logger):
        """
        Обычно логгер настраивается общим образом для всего проекта, поэтому функцию может быть полезным определить.

        НАличие данной функции обязательно, так как она используется в других классах модуля
        """
        pass

    def get_database(self):
        """
        Возвращает имя базы, в которой работаем, на основе значения параметра TEST
        Returns
        -------
        database_name : str
            имя базы данных
        """
        if self.parameters['TEST']:
            database_name = self.cfg_sources['db_backups']['test']
        else:
            database_name = self.cfg_sources['db_backups']['prod']

        return database_name

    def get_table_description(self, table_name):
        """
        Функция выдющая описание таблицы по имени из cfg_souces
        Parameters
        ----------
        table_name: str
            имя таблицы

        Returns
        -------
        database_name : str
            База, где находится таблица
        tbl_name : str
            Имя таблицы
        partitionedby : list
            Партиции
        link : str
            Полный путь к таблице

        """
        database_name = self.get_database()

        tbl_name = self.cfg_sources['backups'][table_name]['table_name']
        partitionedby = self.cfg_sources['backups'][table_name]['partitionedby']
        link = database_name + '.' + tbl_name
        return database_name, tbl_name, partitionedby, link

    def get_table_link(self, table_name, quiet_mode=False):
        """
        Получение ссылки на таблицу по имени
        Parameters
        ----------
        table_name: str
            имя таблицы
        quiet_mode: bool, optional (default=False)
            True -- возвращает table_name, если table_name нет в конфигах
            False -- если table_name нет в конфигах, выбрасывает exception

        Returns
        -------
        link : str
            путь к таблице
        """
        if table_name in self.cfg_sources.keys():
            link = self.cfg_sources[table_name]
        else:
            try:
                _, _, _, link = self.get_table_description(table_name)
            except Exception as msg:
                if quiet_mode:
                    link = table_name
                else:
                    raise msg

        return link

    def get_date(self, date_format, shift=None):
        """
        Функция возврата даты в нужном формате.
        Parameters
        ----------
        date_format : str
            datetime -- возврат даты в формате datetime.date
            %d-%m-%Y -- возврат даты в указанном формате
        shift : int, optional (default=None)
            Количество дней сдвига от текущей даты вычислений
        Returns
        -------
        date : datetime or str
            date_of_price_calculation -- в указанном формате

        """
        date = datetime.strptime(self.parameters['calc_date'], self.calc_date_format).date()
        if shift is not None:
            date -= timedelta(shift)
        if date_format == 'datetime':
            output = date
        else:
            output = date.strftime(date_format)

        return output

    @staticmethod
    def str2bool(string):
        """
        Парсинг str в bool для argparce.
        Parameters
        ----------
        string : str
            строка для преобразования в булевый тип
        Returns
        -------

        """
        if string.lower() in ('yes', 'true', 't', 'y', '1'):
            output = True
        elif string.lower() in ('no', 'false', 'f', 'n', '0'):
            output = False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

        return output

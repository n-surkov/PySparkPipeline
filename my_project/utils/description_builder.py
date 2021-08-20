#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Сборка файлов-описаний модулей

В каждом модуле собираются файлы:

* ``description.md``(текстовое описание алгоритма)
* ``pipeline.dot``(графическая схема вычислений)
* при наличии установленой dot утилиты собирается ``pipeline.png``

Запуск python3 description_builder.py
Место запуска скрипта не имеет значение
"""
import os
import sys
import time
import subprocess
import platform
from importlib import reload

UTILS_PATH = os.path.dirname(__file__)
PROJECT_PATH = os.path.join(UTILS_PATH, '..', '..')
MODULES_PATH = os.path.join(PROJECT_PATH, 'modules')

sys.path.append(PROJECT_PATH)
from modules.config_base import ConfigBase
config = ConfigBase()


def make_descriptions(rel_path='', modules_list=[]):
    """
    Функция сбора документации.

    Рекурсивно обходит всю структуру модулей.

    Parameters
    ----------
    rel_path : str
        путь текущей папки относительно pricing_workflow/modules. Необходим для рекурсивного обхода.
    modules_list : list
        список, в который будут добавляться пути к модулям, для которых удалось собрать документацию.
    """
    current_path = os.path.join(MODULES_PATH, rel_path)

    # Рекурсивно проходим папки в проекте
    for name in sorted(os.listdir(current_path)):
        if '__' in name or 'ipynb' in name:
            continue

        if os.path.isdir(os.path.join(current_path, name)):
            make_descriptions(os.path.join(rel_path, name), modules_list)
            continue

    # Если в папке присутствует module.py -- то эта папка является модулем проекта, можно построить описание
    if 'module.py' in sorted(os.listdir(current_path)):
        # Проверяем, есть ли в модуле Pipeline
        wrong_module = True
        with open(os.path.join(current_path, 'module.py'), 'r') as fo:
            for line in fo.readlines():
                if 'class Pipeline' in line:
                    wrong_module = False
                    break
        if wrong_module:
            return None

        sys.path.append(current_path)
        sys.path.append(PROJECT_PATH)
        time.sleep(0.1)

        # Загружаем модуль проекта
        import module
        reload(module)
        pipline = module.Pipeline(dict(), config)

        # Создаём описание
        with open(os.path.join(current_path, 'README.md'), 'w') as fo:
            fo.write(pipline.get_pipeline_description(True, False))

        # Создаём dot-граф вычислений
        with open(os.path.join(current_path, 'pipeline.dot'), 'w') as fo:
            fo.write(pipline.get_pipeline_graph())

        # Отрисовываем граф, если есть утилита graphviz
        if platform.system() == 'Linux' or 'Darwin':
            if list(subprocess.Popen(['which', 'dot'], stdout=subprocess.PIPE).stdout):
                with open(os.path.join(current_path, 'pipeline.png'), 'w') as fo:
                    proc = subprocess.Popen(['dot', '-Tpng', os.path.join(current_path, 'pipeline.dot')], stdout=fo)
            else:
                print('Can not build pipeline.dot because "dot" is not installed')

        # Удаляем путь к модулю из путей питона для того, чтобы не было конфликтов с дальнейшими импортами
        paths_for_remove = [path for path in sys.path if PROJECT_PATH in path]
        for path in paths_for_remove:
            sys.path.remove(path)
        time.sleep(0.1)

        # Добавляем путь модуля к списку путей
        modules_list.append(rel_path)

    return None


def make_contents(modules_list):
    """
    Функция сборки оглавления

    Названия модулей забираются из первой строчки описания init-файлов модулей

    Parameters
    ----------
    modules_list : list
        список путей к модулям, для которых удалось собрать документацию.
    """
    def split_path(path):
        """
        Функция перегонки пути в list
        """
        main_path, folder = os.path.split(path)
        path_split = [folder]
        for i in range(10):
            if main_path == '':
                break
            main_path, folder = os.path.split(main_path)
            path_split.insert(0, folder)
        return path_split

    # Составление структуры модулей в виде словаря
    tree_dict = {}
    for path in modules_list:
        dct = tree_dict
        for folder in split_path(path):
            dct = dct.setdefault(folder, {})

    concepts = """
<details>
  <summary>Содержание проекта</summary>
  
"""

    def print_point(concepts, local_tree_dict, local_path='', level=1):
        """
        Функция рекурсивного обхода модулей и составления оглавления
        """
        for key, val in local_tree_dict.items():
            new_path = os.path.join(local_path, key)

            # Чтение имени модуля из init
            module_name = ''
            init_path = os.path.join(MODULES_PATH, new_path, '__init__.py')
            with open(init_path, 'r') as fo:
                read_module_name = False
                for line in fo.readlines():
                    if line.strip() == '"""':
                        read_module_name = True
                        continue

                    if read_module_name and line.strip():
                        module_name = line.strip()
                        break

            # Если модуль не в продакшене, то описание не составляем
            if module_name == 'no product':
                continue

            # Заполняем описание в случае конечной точки пути
            if val == {}:
                module_description_path = os.path.join('.', 'modules', new_path, 'README.md')
                concepts += '  ' * level + f'* [{module_name}]({module_description_path})\n'
                continue

            # Заполняем описание в случае промежуточной точки пути
            if isinstance(val, dict):
                concepts += '  ' * level + f'* **{module_name}**\n'
                concepts += print_point('', val, new_path, level + 1)

        return concepts

    concepts += print_point('', tree_dict)
    concepts += '</details>\n\n'

    return concepts.strip()


if __name__ == '__main__':
    correct_modules_list = []
    make_descriptions(modules_list=correct_modules_list)
    concepts = make_contents(correct_modules_list)
    # Обновляем README проекта
    readme = ''
    details_line = ''
    old_concepts = False
    with open(os.path.join(PROJECT_PATH, 'README.md'), 'r') as fo:
        for line in fo.readlines():
            # print(details_line)
            # print(old_concepts)
            if '<details>' in line:
                details_line = line
                continue

            if details_line:
                if 'Содержание проекта' in line:
                    old_concepts = True
                    details_line = ''
                    continue
                else:
                    readme += details_line
                    details_line = ''

            if old_concepts:
                if '</details>' not in line:
                    continue
                else:
                    old_concepts = False
                    readme += concepts + '\n'
                    continue

            readme += line

    with open(os.path.join(PROJECT_PATH, 'README.md'), 'w') as fo:
        fo.write(readme)

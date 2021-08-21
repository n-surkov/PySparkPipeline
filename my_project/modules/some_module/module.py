import pyspark.sql.functions as F
from modules.module_base import StepBase, PipelineBase


class SomeStep(StepBase):
    # название шага и его описание обязательно должны быть разделены пустой строкой для корректного парсинга
    """
    Название шага

    Описание вычислений класса в формате Markdown
    """
    source_tables = {  # словарь описаний источников
        'input_table_1': {  # имя, по которому можно будет обращаться к таблице при описании вычислений
            'link': 'sales_tbl',  # ссылка на таблицу (можно алиасом из config_sources, как здесь),
            # 'argument' если таблица не из БД
            'description': 'Таблица для теста',  # описание таблицы
            'columns': [  # список колонок
                ('zrtpluid', 'string', 'plu', None),  # имя, тип колонки в таблице, новое имя, новый тип
                ('zprice', 'double', 'price', None),
            ]
        }
    }

    output_tables = {
        'out_table_1': {
            'link': None,  # ссылка на таблицу (можно алиасом из config_sources), для дальнейшего сохранения
            'description': 'Таблица после теста',
            'columns': [
                ('plu', 'string'),  # имя и тип колонки в выхоной таблице таблице
                ('price', 'double'),
                ('double_price', 'double'),
            ]
        }
    }

    def _calculations(self):
        "Вычисления класса, на выходе словарь в соответствии с self.output_tables"
        df = (
            self.tables['input_table_1']
                .withColumn('double_price', F.col('price') * 2)
                .withColumn('price', F.round('price', 0))
        )
        return {'out_table_1': df}


class AnotherStep(StepBase):
    """
    Шаг, описание которого осталось за скобками

    Делает то же, что и предыдущий:
    * удавивает цену, записывает результат в колонку `double_price`
    """
    source_tables = {  # словарь описаний источников
        'out_table_1': {  # имя, по которому можно будет обращаться к таблице при описании вычислений
            'link': 'argument',  # ссылка на таблицу (можно алиасом из config_sources),
            # 'argument' если таблица не из БД
            'description': 'Таблица для теста',  # описание таблицы
            'columns': [  # список колонок
                ('plu', 'string', 'plu', None),  # имя, тип колонки в таблице, новое имя, новый тип
                ('price', 'double', 'price', None),
            ]
        }
    }

    output_tables = {
        'out_table_2': {
            'link': None,  # ссылка на таблицу (можно алиасом из config_sources), для дальнейшего сохранения
            'description': 'Таблица после теста',
            'columns': [
                ('plu', 'string'),  # имя и тип колонки в выхоной таблице таблице
                ('price', 'double'),
                ('double_price', 'double'),
            ]
        }
    }

    def _calculations(self):
        "Вычисления класса, на выходе словарь в соответствии с self.output_tables"
        df = (
            self.tables['out_table_1']
                .withColumn('double_price', F.col('price') * 2)
                .withColumn('price', F.round('price', 0))
        )
        # Проверяем факт попадания параметра --vas в конфиг
        if self.config['validateAS'] == 1:
            self.logger.debug('Вычисления провалидированы')
        else:
            self.logger.debug('Вычисления не валидировались')

        return {'out_table_2': df}


# Определяем пайплайн
class Pipeline(PipelineBase):
    """
    Название пайплана

    Общее описание всего пайплайна вычислений
    """
    step_sequence = [
        SomeStep,
        AnotherStep
    ]

    output_tables = {  # Аналогично описанию в Step
        'out_table_2': {
            'link': None,
            'description': 'Финальная таблица',
            'columns': [
                ('plu', 'string'),
                ('double_price', 'double'),
            ]
        }
    }

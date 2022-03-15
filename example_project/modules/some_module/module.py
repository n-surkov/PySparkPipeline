import pyspark.sql.functions as F
from pyspark.sql import Window
from modules.module_base import StepBase, PipelineBase


class Processing(StepBase):
    """
    Предобработка данных датасета "Титаник"
    
    С данными из датасета "Титаник" производятся следующие операции:
    * Для каждого класса билетов вычисляется средний возраст пассажиров, купивших билет
    * Если возраст пассажира не указан, то он заменяется средним возрастом пассажиров, 
    купивших билет такого же класса (с округлением до целого)
    """
    source_tables = {  # словарь описаний источников
        'input_table_1': {  # имя, по которому можно будет обращаться к таблице при описании вычислений
            'link': 'titanic_tbl',  # ссылка на таблицу (можно алиасом из config_sources, как здесь), 
                                    # 'argument' если таблица не из БД
            'description': 'Данные пассажиров титаника',  # описание таблицы
            'columns': [  # список колонок
                ('Survived', 'bigint', 'survived', 'int'), # имя, тип колонки в таблице, новое имя (опционально), новый тип (опционально)
                ('Pclass', 'bigint', 'ticket_class', 'int'),
                ('Sex', 'string', 'sex'),
                ('Age', 'double', 'age'),
            ]
        }
    }
    
    output_tables = {
        'out_table_1': {
            'link': None,  # ссылка на таблицу (можно алиасом из config_sources), для дальнейшего сохранения
            'description': 'Обработанные данные пассажиров титаника',
            'columns': [
                ('survived', 'int'),  # имя и тип колонки в выхоной таблице
                ('sex', 'string'),
                ('age', 'double'),
            ]
        }
    }
    
    def _calculations(self):
        "Вычисления класса, на выходе словарь в соответствии с self.output_tables"
        w = Window.partitionBy('ticket_class')
        df = (
            self.tables['input_table_1']
            .withColumn('avg_age', F.mean('age').over(w))
            .withColumn('age', F.when(F.col('age').isNull(), F.round(F.col('avg_age'), 0)).otherwise(F.col('age')))
        )
        return {'out_table_1': df}


class CalcStats(StepBase):
    """
    Вычисление статистик по пассажирам Титаника

    Данные таблицы `out_table_1` группируются по полу пассажира и вычисляются следующие статистики:
    * процент выживаемости
    * средний возраст
    """
    source_tables = {  # словарь описаний источников
        'out_table_1': {  # имя, по которому можно будет обращаться к таблице при описании вычислений
            'link': 'argument',  # ссылка на таблицу (можно алиасом из config_sources),
            # 'argument' если таблица не из БД
            'description': 'Обработанные данные пассажиров титаника',  # описание таблицы
            'columns': [  # список колонок
                ('survived', 'int'),  # имя, тип колонки в таблице, новое имя (опционально), новый тип (опционально)
                ('sex', 'string'),
                ('age', 'double'),
            ]
        }
    }

    output_tables = {
        'out_table_2': {
            'link': None,  # ссылка на таблицу (можно алиасом из config_sources), для дальнейшего сохранения
            'description': 'Статистики пассажиров титаника',
            'columns': [
                ('sex', 'string'),  # имя и тип колонки в выхоной таблице
                ('avg_age', 'double'),  
                ('survival_percentage', 'double')
            ]
        }
    }

    def _calculations(self):
        "Вычисления класса, на выходе словарь в соответствии с self.output_tables"
        df = (
            self.tables['out_table_1']
            .groupby('sex')
            .agg(
                F.mean('age').alias('avg_age'),
                F.count('survived').cast('double').alias('pass_cnt'),
                F.sum('survived').cast('double').alias('survived_cnt')
            )
            .withColumn('survival_percentage', F.round(100 * F.col('survived_cnt') / F.col('pass_cnt'), 2))
        )
        # Пример использования параметров конфига в шаге. Проверяем факт попадания параметра --vas в конфиг.
        if self.config['validateAS'] == 1:
            self.logger.debug('Вычисления провалидированы')
        else:
            self.logger.debug('Вычисления не валидировались')

        return {'out_table_2': df}


# Определяем пайплайн
class Pipeline(PipelineBase):
    """
    Пайплайн расчёта характеристик пассажиров титаника

    Расчёт характеристик соответствующих каждому полу пассажиров титаника:
    * процент выживаемости
    * средний возраст
    """
    step_sequence = [
        Processing,
        CalcStats
    ]

    output_tables = {  # Аналогично описанию в Step (Опционально можно добавить описание колонки)
        'out_table_2': {
            'link': None,
            'description': 'Финальная таблица',
            'columns': [
                ('sex', 'string', 'Пол'),
                ('avg_age', 'double'),
                ('survival_percentage', 'double', 'Процент выживших')
            ]
        }
    }

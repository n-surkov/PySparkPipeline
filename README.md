# PySparkPipeline
Модуль для работы со спарком в концепции пайплайна

Подробное описание работы с модулем и его бенефиты описаны в тетрадке article.ipynb

Копия [здесь](https://habr.com/ru/company/X5Group/blog/579232/).

# Краткая инструкция

## 1. Установка пакета
### Remote

```bash
$ python3 -m pip install git+ssh://git@github.com:n-surkov/PySparkPipeline.git
```

### Local

```bash
$ git clone https://github.com/n-surkov/PySparkPipeline.git
$ python3 -m pip install -e ./PySparkPipeline
```

## 2. Определение классов под свой проект

Для своего конкретного проекта лучше доопределить базовые классы. Пример:
* `example_project/modules/config_base.py`
* `example_project/modules/module_base.py`

В библиотечном файле конфига отсутствует функция настройки логгера. 
Но в файле `example_project/modules/config_base.py` она определена таким образом, 
что пишет логи в путь `data/logs`.

Если забирать `example_project/modules/config_base.py`, то обязательно нужно создать 
директорию `data/logs`.

Так же в файле `example_project/modules/config_base.py` определяются пути к конфигам. 
Если они будут лежать не в корне проекта, то пути нужно будет так же заменить.

## 3. Использование

Иерархию модулей лучше формировать согласно `example_project`. 
Тогда легко будет собирать документацию.

По пути `example_project/modules/some_module` лежит пример использования базовых классов.

## 4. Документация

Если иерархия проекта будет согласована с `example_project`, 
то можно добавить в него файл `example_project/utils/description_builder.py`. 
Этот скрипт будет собирать документацию в автоматическом режиме.

При этом в корень проекта нужно положить README.md с обязательным содержанием 

```
<details>
  <summary>Содержание проекта</summary>
  
</details>
```

В эту часть будет выкидываться оглавление.

## 5. Тестирование

```
python3 -m unittest tests/test_module_base.py
```
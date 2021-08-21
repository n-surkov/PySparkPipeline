import os

# Импорт конфигов
MODULES_BASE_PATH = os.path.dirname(__file__)
CFG_PATH = os.path.join(MODULES_BASE_PATH, '../my_project', 'config.yml')
CFG_SOURCES = os.path.join(MODULES_BASE_PATH, '../my_project', 'config_sources.yml')

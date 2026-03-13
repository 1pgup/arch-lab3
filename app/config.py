from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml


# BASE_DIR — это папка, в которой находится текущий Python-файл.
BASE_DIR = Path(__file__).resolve().parent

# Полный путь к YAML-файлу конфигурации.
CONFIG_PATH = BASE_DIR / "config.yaml"


class Config:
    """Небольшой объект конфигурации.

    Конфигурация хранится в YAML, чтобы её было легко читать и менять вручную.
    """

    def __init__(self, raw: dict[str, Any]) -> None:
        # Сохраняем исходный словарь конфигурации целиком.
        self.raw = raw

        # Извлекаем вложенные секции конфигурации.
        database = raw.get("database", {})
        sync = raw.get("sync", {})
        scheduler = sync.get("scheduler", {})

        # Путь к файлу базы данных.
        # Если в YAML ничего не указано, используется значение по умолчанию.
        self.database_path = database.get("path", "./data/exchange_rates.db")

        # Список валют, которые нужно синхронизировать автоматически.
        self.sync_currencies = [code.upper() for code in sync.get("currencies", ["USD", "EUR"])]

        # Режим работы планировщика.
        # Например:
        # - "cron" — запуск в конкретное время;
        # - "interval" — запуск через фиксированный интервал.
        self.scheduler_mode = scheduler.get("mode", "cron")

        # Интервал между запусками в секундах для режима interval.
        # По умолчанию 86400 секунд = 1 сутки.
        self.scheduler_interval_seconds = int(scheduler.get("interval_seconds", 86400))

        # Час запуска для режима cron.
        self.scheduler_hour = int(scheduler.get("hour", 0))

        # Минута запуска для режима cron.
        self.scheduler_minute = int(scheduler.get("minute", 1))

        # Сколько дней назад можно попробовать искать данные,
        # если daily.txt за целевую дату недоступен.
        self.daily_retry_days = int(sync.get("daily_retry_days", 3))


def load_config(path: Path = CONFIG_PATH) -> Config:
    # Открываем YAML-файл конфигурации на чтение.
    with path.open("r", encoding="utf-8") as file:
        # Безопасно разбираем YAML в обычный Python-словарь.
        raw = yaml.safe_load(file) or {}

    # На основе словаря создаём объект Config
    return Config(raw)

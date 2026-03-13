from __future__ import annotations

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .config import Config
from .services import sync_daily_rates

# Создаём логгер для текущего модуля.
logger = logging.getLogger(__name__)


def create_scheduler(connection, config: Config) -> BackgroundScheduler:
    """Создаёт и настраивает планировщик фоновой синхронизации."""

    # Создаём экземпляр фонового планировщика.
    # BackgroundScheduler работает в фоне внутри приложения
    # и может запускать задачи по времени или по интервалу.
    scheduler = BackgroundScheduler()

    def scheduled_job() -> None:
        from datetime import date

        # Выполняем синхронизацию курсов за текущую дату.
        # Если данных за сегодня ещё нет, логика внутри sync_daily_rates
        # может попробовать взять данные за предыдущие дни
        # в пределах значения retry_days.
        result = sync_daily_rates(
            connection=connection,
            target_date=date.today(),
            currencies=config.sync_currencies,
            retry_days=config.daily_retry_days,
        )

        logger.info("Scheduled sync result: %s", result)

    # Выбираем тип запуска задачи в зависимости от конфигурации.
    #
    # Вариант 1: interval
    # Задача будет запускаться через равные промежутки времени,
    # например каждые 60 секунд.
    if config.scheduler_mode == "interval":
        trigger = IntervalTrigger(seconds=config.scheduler_interval_seconds)

    # Вариант 2: cron
    # Задача будет запускаться в конкретное время суток,
    # например каждый день в 00:01.
    else:
        trigger = CronTrigger(hour=config.scheduler_hour, minute=config.scheduler_minute)

    # Регистрируем задачу в планировщике.
    # scheduled_job — функция, которую нужно запускать.
    # trigger — правило запуска (по интервалу или по расписанию).
    # id — уникальный идентификатор задачи внутри планировщика.
    # replace_existing=True — если задача с таким id уже есть,
    # она будет заменена новой, а не продублирована.
    scheduler.add_job(
        scheduled_job,
        trigger=trigger,
        id="daily_sync_job",
        replace_existing=True,
    )

    # Возвращаем готовый планировщик.
    # Сам по себе он ещё не работает — его нужно отдельно запустить через scheduler.start().
    return scheduler

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import logging
import sqlite3

from .cnb_client import CNBClient
from .database import get_report_rows, upsert_rate


logger = logging.getLogger(__name__)


def sync_daily_rates(
    connection: sqlite3.Connection,
    target_date: date,
    currencies: list[str],
    retry_days: int = 3,
) -> dict:
    """Синхронизирует ежедневные курсы.

    Если за целевую дату данных нет, пробуем несколько предыдущих дней.
    """

    # Здесь будем накапливать предупреждения:
    # например, если файл за конкретную дату недоступен.
    warnings: list[str] = []

    # Счётчик успешно сохранённых записей в БД.
    inserted = 0

    # Дата, за которую в итоге реально удалось получить данные.
    # Может отличаться от target_date, если пришлось откатываться назад.
    used_date: str | None = None

    # Пробуем получить daily.txt за target_date.
    # Если не получилось, идём на 1 день назад, потом ещё на 1 и так далее,
    # пока не исчерпаем retry_days.
    for offset in range(retry_days + 1):
        actual_date = target_date - timedelta(days=offset)
        try:
            # Пытаемся получить курсы за конкретную дату.
            daily_rates = CNBClient.fetch_daily(actual_date)

            # Запоминаем дату, за которую реально удалось получить данные.
            used_date = actual_date.isoformat()
            break
        except Exception as exc:
            # Если файл недоступен или возникла ошибка разбора,
            # не завершаем работу сразу, а просто записываем предупреждение
            # и пробуем предыдущую дату.
            warnings.append(f"Не удалось получить daily.txt за {actual_date.isoformat()}: {exc}")
            daily_rates = []

    # Если после всех попыток данные так и не удалось получить,
    # возвращаем warning-ответ без падения приложения.
    if not daily_rates:
        return {
            "status": "warning",
            "inserted": 0,
            "used_date": used_date,
            "warnings": warnings,
        }

    # Приводим список валют к верхнему регистру и превращаем в множество.
    currency_set = {code.upper() for code in currencies}

    # Обрабатываем все строки курсов, пришедшие из daily.txt.
    for rate in daily_rates:
        # Если текущая валюта не входит в список нужных,
        # пропускаем её.
        if rate.currency_code not in currency_set:
            continue

        # Сохраняем курс в БД.
        # Используется upsert-логика:
        # если запись уже есть, она обновится;
        # если нет — будет создана.
        upsert_rate(
            connection,
            {
                "rate_date": rate.rate_date,
                "currency_code": rate.currency_code,
                "country": rate.country,
                "currency_name": rate.currency_name,
                "amount": rate.amount,
                "rate": rate.rate,
                "rate_per_unit": rate.rate_per_unit,
                "source": rate.source,
            },
        )

        # Увеличиваем счётчик сохранённых записей.
        inserted += 1

    logger.info("Daily sync completed: target_date=%s, used_date=%s, inserted=%s", target_date, used_date, inserted)

    # Возвращаем результат работы функции.
    return {
        "status": "ok",
        "inserted": inserted,
        "used_date": used_date,
        "warnings": warnings,
    }


def sync_range_rates(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
    currencies: list[str],
) -> dict:
    """Синхронизирует данные за период через year.txt.

    Подход простой: определяем все годы в диапазоне, загружаем соответствующие year.txt,
    затем оставляем только нужные даты и валюты.
    """

    # Проверяем корректность диапазона дат.
    # Начальная дата не должна быть позже конечной.
    if start_date > end_date:
        raise ValueError("startDate не может быть больше endDate")

    # Список предупреждений.
    warnings: list[str] = []

    # Счётчик всех записей, которые были сохранены в БД.
    inserted = 0

    # Список годов, которые реально были обработаны.
    processed_years: list[int] = []

    # Нормализуем список валют для удобной фильтрации.
    currency_set = {code.upper() for code in currencies}

    # Проходим по всем годам, которые попадают в заданный диапазон.
    for year in range(start_date.year, end_date.year + 1):
        processed_years.append(year)
        try:
            # Загружаем годовой файл year.txt за текущий год.
            year_rates = CNBClient.fetch_year(year)
        except Exception as exc:
            # Если файл за год не удалось обработать,
            # просто фиксируем предупреждение и продолжаем со следующим годом.
            warnings.append(f"Не удалось обработать year.txt за {year}: {exc}")
            continue

        # Перебираем все курсы, найденные в year.txt.
        for rate in year_rates:
            rate_dt = date.fromisoformat(rate.rate_date)

            # Оставляем только те записи, которые попадают в нужный диапазон дат.
            if rate_dt < start_date or rate_dt > end_date:
                continue

            # Оставляем только нужные валюты.
            if rate.currency_code not in currency_set:
                continue

            # Сохраняем запись в БД.
            upsert_rate(
                connection,
                {
                    "rate_date": rate.rate_date,
                    "currency_code": rate.currency_code,
                    "country": rate.country,
                    "currency_name": rate.currency_name,
                    "amount": rate.amount,
                    "rate": rate.rate,
                    "rate_per_unit": rate.rate_per_unit,
                    "source": rate.source,
                },
            )

            # Увеличиваем счётчик добавленных/обновлённых записей.
            inserted += 1

    # Возвращаем итог по синхронизации диапазона.
    # Если ни одной записи не вставлено, статус будет warning.
    return {
        "status": "ok" if inserted > 0 else "warning",
        "inserted": inserted,
        "processed_years": processed_years,
        "warnings": warnings,
    }


def build_report(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
    currencies: list[str],
) -> dict:
    """Формирует JSON-отчёт.

    Статистика строится по rate_per_unit, то есть всегда для 1 условной единицы валюты.
    Например, если ČNB публикует 100 JPY, то в БД хранится уже нормализованное значение для 1 JPY.
    """

    # Проверяем корректность диапазона дат.
    if start_date > end_date:
        raise ValueError("startDate не может быть больше endDate")

    # Нормализуем входной список валют:
    # убираем пробелы, приводим к верхнему регистру, отбрасываем пустые значения.
    normalized_currencies = [code.strip().upper() for code in currencies if code.strip()]

    # Получаем агрегированные строки отчёта из БД.
    rows = get_report_rows(connection, start_date.isoformat(), end_date.isoformat(), normalized_currencies)

    # Здесь будет итоговый словарь с результатами по каждой валюте.
    results: dict[str, dict] = {}

    # Множество валют, для которых в БД реально нашлись данные.
    found_currencies = set()

    # Обрабатываем строки, пришедшие из БД.
    for row in rows:
        found_currencies.add(row["currency_code"])

        # Для каждой валюты сохраняем статистику в удобном JSON-виде.
        # round(..., 6) ограничивает количество знаков после запятой,
        # чтобы ответ выглядел аккуратнее.
        results[row["currency_code"]] = {
            "min": round(row["min_rate"], 6),
            "max": round(row["max_rate"], 6),
            "avg": round(row["avg_rate"], 6),
            "samples": row["samples_count"],
            "first_available_date": row["first_date"],
            "last_available_date": row["last_date"],
        }

    # Сюда собираем предупреждения по валютам,
    # для которых за указанный период нет данных в БД.
    warnings: list[str] = []

    for currency in normalized_currencies:
        if currency not in found_currencies:
            warnings.append(
                f"Для валюты {currency} в БД нет данных за период {start_date.isoformat()} - {end_date.isoformat()}"
            )

            # Даже если данных нет, добавляем валюту в results,
            # чтобы в ответе была единая структура по всем запрошенным кодам.
            results[currency] = {
                "min": None,
                "max": None,
                "avg": None,
                "samples": 0,
                "first_available_date": None,
                "last_available_date": None,
            }

    # Возвращаем готовый JSON-отчёт:
    # период, список валют, результаты и предупреждения.
    return {
        "period": {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        "currencies": normalized_currencies,
        "results": results,
        "warnings": warnings,
    }

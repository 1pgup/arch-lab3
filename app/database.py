from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def get_connection(db_path: str) -> sqlite3.Connection:
    """Создаёт подключение к SQLite.

    row_factory нужен, чтобы можно было читать строки как словари по имени столбца.
    """

    # Создаём родительскую папку для файла БД, если её ещё нет.
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Открываем соединение с SQLite.
    connection = sqlite3.connect(db_path, check_same_thread=False)

    # Настраиваем формат возвращаемых строк.
    # Благодаря sqlite3.Row к полям можно обращаться по имени столбца:
    # row["currency_code"], row["rate_date"] и т.д.
    connection.row_factory = sqlite3.Row

    return connection


def init_db(connection: sqlite3.Connection) -> None:
    """Создаёт таблицу, если БД запускается впервые."""

    # Создаём основную таблицу для хранения курсов валют.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_date TEXT NOT NULL,
            currency_code TEXT NOT NULL,
            country TEXT,
            currency_name TEXT,
            amount INTEGER NOT NULL,
            rate REAL NOT NULL,
            rate_per_unit REAL NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(rate_date, currency_code)
        )
        """
    )

    # Подтверждаем изменения в БД.
    connection.commit()


def upsert_rate(connection: sqlite3.Connection, record: dict[str, Any]) -> None:
    """Добавляет или обновляет курс за конкретную дату и валюту."""

    # Выполняем вставку новой записи в таблицу.
    #
    # UNIQUE(rate_date, currency_code) в структуре таблицы означает,
    # что для одной даты и одной валюты может существовать только одна запись.
    #
    # Если такой записи ещё нет — она будет добавлена.
    # Если запись уже есть — сработает блок ON CONFLICT ... DO UPDATE,
    # и данные будут обновлены.
    connection.execute(
        """
        INSERT INTO exchange_rates (
            rate_date,
            currency_code,
            country,
            currency_name,
            amount,
            rate,
            rate_per_unit,
            source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(rate_date, currency_code)
        DO UPDATE SET
            country = excluded.country,
            currency_name = excluded.currency_name,
            amount = excluded.amount,
            rate = excluded.rate,
            rate_per_unit = excluded.rate_per_unit,
            source = excluded.source,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            # Передаём значения параметров в SQL-запрос.
            # Такой способ безопаснее и удобнее, чем собирать SQL-строку вручную.
            record["rate_date"],
            record["currency_code"],
            record["country"],
            record["currency_name"],
            record["amount"],
            record["rate"],
            record["rate_per_unit"],
            record["source"],
        ),
    )

    # Подтверждаем изменения после вставки или обновления.
    connection.commit()


def get_report_rows(
    connection: sqlite3.Connection,
    start_date: str,
    end_date: str,
    currencies: list[str],
) -> list[sqlite3.Row]:
    """Собирает агрегаты для отчёта.

    Статистика строится только по тем строкам, которые реально есть в БД.
    Это позволяет не падать на пропусках: пропуски просто не участвуют в расчёте.
    """

    # Для SQL-условия IN (?, ?, ?, ...)
    # нужно заранее сгенерировать нужное количество плейсхолдеров.
    # Их число зависит от количества валют в списке currencies.
    placeholders = ",".join("?" for _ in currencies)

    # Формируем SQL-запрос:
    # - берём только записи в нужном диапазоне дат;
    # - оставляем только нужные валюты;
    # - считаем агрегаты по каждой валюте:
    #   минимум, максимум, среднее, количество записей;
    # - дополнительно определяем первую и последнюю доступную дату в выборке.
    query = f"""
        SELECT
            currency_code,
            MIN(rate_per_unit) AS min_rate,
            MAX(rate_per_unit) AS max_rate,
            AVG(rate_per_unit) AS avg_rate,
            COUNT(*) AS samples_count,
            MIN(rate_date) AS first_date,
            MAX(rate_date) AS last_date
        FROM exchange_rates
        WHERE rate_date BETWEEN ? AND ?
          AND currency_code IN ({placeholders})
        GROUP BY currency_code
        ORDER BY currency_code
    """

    # Выполняем запрос и передаём параметры:
    # сначала начало и конец периода,
    # затем список валют.
    #
    # fetchall() возвращает все найденные строки сразу.
    return connection.execute(query, [start_date, end_date, *currencies]).fetchall()

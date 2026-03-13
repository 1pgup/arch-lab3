from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
import logging
from fastapi import FastAPI, HTTPException, Query
import logging
from contextlib import asynccontextmanager
from datetime import date

from .config import load_config
from .database import get_connection, init_db
from .scheduler import create_scheduler
from .schemas import SyncDailyRequest, SyncRangeRequest
from .services import build_report, sync_daily_rates, sync_range_rates


from fastapi import FastAPI, HTTPException, Query

# Настраиваем базовое логирование для всего приложения.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Создаём логгер для текущего файла.
logger = logging.getLogger(__name__)


# Загружаем настройки приложения из конфигурационного файла.
config = load_config()

# Открываем соединение с базой данных.
connection = get_connection(config.database_path)

# Создаём таблицы в БД, если они ещё не существуют.
init_db(connection)

# Создаём объект планировщика, который будет выполнять
# автоматическую синхронизацию по расписанию.
scheduler = create_scheduler(connection, config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Этот блок выполняется при старте и остановке приложения FastAPI.
    # Он нужен, чтобы корректно запускать и завершать фоновые ресурсы.

    # При старте приложения запускаем фоновый планировщик.
    # После этого он начинает выполнять задачи синхронизации по расписанию.
    scheduler.start()
    logger.info("Scheduler started")

    try:
        # yield передаёт управление самому приложению.
        # Пока приложение работает, код остаётся в этом месте.
        yield
    finally:
        # При завершении приложения останавливаем планировщик,
        # чтобы фоновые задачи больше не запускались.
        scheduler.shutdown(wait=False)

        # Закрываем соединение с базой данных.
        connection.close()

        logger.info("Application stopped")


# Создаём объект FastAPI и задаём базовую информацию о сервисе.
app = FastAPI(
    title="CZK Sync and Report Service",
    description="Учебный сервис синхронизации и отчётов по курсам валют ČNB",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    # Служебный эндпоинт для проверки,
    # что приложение запущено и отвечает на запросы.
    return {
        "status": "ok",
        "scheduler_mode": config.scheduler_mode,
        "currencies": config.sync_currencies,
    }


@app.post("/sync/daily")
def sync_daily(request: SyncDailyRequest) -> dict:
    # Если дата не передана в запросе, используем сегодняшнюю.
    target_date = request.date_value or date.today()

    try:
        # Запускаем синхронизацию курсов за один день.
        return sync_daily_rates(
            connection=connection,
            target_date=target_date,
            currencies=config.sync_currencies,
            retry_days=config.daily_retry_days,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/sync/range")
def sync_range(request: SyncRangeRequest) -> dict:
    try:
        # Запускаем синхронизацию за диапазон дат.
        return sync_range_rates(
            connection=connection,
            start_date=request.start_date,
            end_date=request.end_date,
            currencies=config.sync_currencies,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/report")
def get_report(
    start_date: date = Query(alias="startDate"),
    end_date: date = Query(alias="endDate"),
    currencies: str = Query(..., description="Список валют через запятую, например USD,EUR,PLN"),
) -> dict:
    try:
        # Преобразуем строку вида "USD,EUR,PLN" в список:
        # ["USD", "EUR", "PLN"].
        currency_list = [code.strip().upper() for code in currencies.split(",") if code.strip()]

        # Проверяем, что после разбора списка валют не получилось пустое значение.
        if not currency_list:
            raise ValueError("Нужно передать хотя бы одну валюту")

        # Формируем отчёт по выбранным валютам и диапазону дат.
        return build_report(
            connection=connection,
            start_date=start_date,
            end_date=end_date,
            currencies=currency_list,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

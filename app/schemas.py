from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


# Модель запроса для ручной синхронизации данных за диапазон дат.
# Используется в эндпоинте, куда клиент передаёт начальную и конечную дату.
class SyncRangeRequest(BaseModel):
    # Дата начала периода.
    # alias="startDate" означает, что во входном JSON ожидается поле startDate,
    # а внутри Python-кода оно будет доступно как start_date.
    start_date: date = Field(alias="startDate")

    # Дата конца периода.
    # По аналогии с полем выше:
    # во внешнем запросе используется имя endDate,
    # а внутри приложения — end_date.
    end_date: date = Field(alias="endDate")


# Модель запроса для синхронизации данных за один день.
class SyncDailyRequest(BaseModel):
    # Дата, за которую нужно выполнить синхронизацию.
    # Поле необязательное, потому что default=None.
    # Если клиент не передаст дату, приложение сможет,
    # например, взять текущую дату автоматически.
    #
    # alias="date" означает, что JSON должен содержать поле date,
    # а в коде оно будет называться date_value.
    date_value: Optional[date] = Field(default=None, alias="date")

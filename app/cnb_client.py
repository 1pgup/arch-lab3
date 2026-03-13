from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


# Шаблон URL для получения daily.txt за конкретную дату.
# В {date} позже подставляется дата в формате dd.mm.yyyy.
DAILY_URL = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt?date={date}"

# Шаблон URL для получения year.txt за конкретный год.
# В {year} позже подставляется, например, 2025.
YEAR_URL = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/year.txt?year={year}"


@dataclass
class ParsedRate:
    # Дата курса в формате YYYY-MM-DD.
    rate_date: str

    # Код валюты, например USD, EUR, JPY.
    currency_code: str

    # Страна валюты.
    country: str

    # Название валюты.
    currency_name: str

    # Количество единиц валюты, к которым относится курс.
    amount: int

    # Исходное значение курса, как его отдаёт ČNB.
    rate: float

    # Нормализованный курс за 1 единицу валюты.
    rate_per_unit: float

    # Источник данных: daily.txt или year.txt.
    source: str


class CNBClient:
    """Клиент для загрузки и разбора текстовых файлов ČNB."""

    @staticmethod
    def _download_text(url: str) -> str:
        try:
            # Открываем URL и читаем ответ.
            with urlopen(url, timeout=30) as response:
                return response.read().decode("utf-8")

        except HTTPError as exc:
            raise RuntimeError(f"Ошибка HTTP при обращении к ČNB: {exc.code} {exc.reason}") from exc

        except URLError as exc:
            raise RuntimeError(f"Ошибка сети при обращении к ČNB: {exc.reason}") from exc

    @staticmethod
    def _parse_float(value: str) -> float:
        return float(value.replace(",", ".").strip())

    @staticmethod
    def fetch_daily(target_date: date) -> list[ParsedRate]:
        """Загружает файл ежедневного курса за конкретную дату."""

        # Преобразуем дату к формату, который ожидает ČNB в URL
        date_for_url = target_date.strftime("%d.%m.%Y")

        # Загружаем содержимое daily.txt.
        text = CNBClient._download_text(DAILY_URL.format(date=date_for_url))

        # Разбиваем текст на строки, убираем пробелы по краям
        # и отбрасываем пустые строки.
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        # Ожидаем, что в daily.txt будут хотя бы:
        # заголовок с датой,
        # строка с названиями колонок,
        # и хотя бы одна строка данных.
        if len(lines) < 3:
            raise RuntimeError("ČNB вернул неожиданный формат daily.txt")

        # Пример первой строки:
        # 26 Jul 2019 #143
        # Отсюда можно попробовать извлечь дату файла.
        header_line = lines[0]
        try:
            # Берём часть до символа # и парсим дату.
            parsed_date = datetime.strptime(header_line.split("#")[0].strip(), "%d %b %Y").date()
        except ValueError:
            # Если формат заголовка неожиданно изменится,
            # просто используем дату, которую запрашивали.
            parsed_date = target_date

        # Сюда будем собирать разобранные записи о курсах.
        result: list[ParsedRate] = []

        # В daily.txt первые две строки — служебные,
        # поэтому реальные данные начинаются с lines[2:].
        for line in lines[2:]:
            # Каждая строка разделяется символом "|".
            parts = line.split("|")

            # Ожидаемый формат строки:
            # country | currency_name | amount | currency_code | rate
            # Если колонок не 5, строка считается аномальной и пропускается.
            if len(parts) != 5:
                continue

            country, currency_name, amount, currency_code, rate = parts

            # Преобразуем количество валюты в целое число.
            amount_int = int(amount.strip())

            # Преобразуем курс из текстового вида в float.
            rate_value = CNBClient._parse_float(rate)

            # Добавляем нормализованную запись в результат.
            result.append(
                ParsedRate(
                    rate_date=parsed_date.isoformat(),
                    currency_code=currency_code.strip().upper(),
                    country=country.strip(),
                    currency_name=currency_name.strip(),
                    amount=amount_int,
                    rate=rate_value,
                    rate_per_unit=rate_value / amount_int,
                    source="daily.txt",
                )
            )

        return result

    @staticmethod
    def fetch_year(year: int) -> list[ParsedRate]:
        """Загружает годовой файл и преобразует его в список записей.

        В year.txt каждая колонка выглядит как "1 USD" или "100 JPY".
        Поэтому сначала нужно разобрать шапку и извлечь код валюты и amount.
        """

        # Загружаем содержимое year.txt за нужный год.
        text = CNBClient._download_text(YEAR_URL.format(year=year))

        # Подготавливаем строки так же, как и для daily.txt:
        # убираем пробелы по краям и пустые строки.
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        # В year.txt ожидается хотя бы строка заголовков
        # и хотя бы одна строка с данными.
        if len(lines) < 2:
            raise RuntimeError("ČNB вернул неожиданный формат year.txt")

        # Первая строка содержит заголовки колонок.
        headers = lines[0].split("|")

        # Ожидаем, что первая колонка называется Date.
        if not headers or headers[0] != "Date":
            raise RuntimeError("В year.txt не найден ожидаемый заголовок")

        # В этом списке сохраним метаданные по колонкам:
        # (код валюты, amount)
        header_meta: list[tuple[str, int]] = []

        # Пропускаем первую колонку Date и разбираем остальные.
        for column in headers[1:]:
            amount_text, currency_code = column.split(" ")
            header_meta.append((currency_code.strip().upper(), int(amount_text.strip())))

        # Итоговый список разобранных курсов.
        result: list[ParsedRate] = []

        # Обрабатываем каждую строку с данными.
        for line in lines[1:]:
            parts = line.split("|")

            # Если число колонок не совпадает с заголовком,
            # строка считается некорректной и пропускается.
            # Это мягкая обработка ошибок без падения всего процесса.
            if len(parts) != len(headers):
                continue

            # Первая колонка содержит дату в формате dd.mm.yyyy.
            # Преобразуем её к ISO-формату YYYY-MM-DD.
            rate_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date().isoformat()

            # Перебираем значения по валютным колонкам.
            for index, raw_value in enumerate(parts[1:]):
                # Берём метаданные текущей колонки:
                # код валюты и количество единиц.
                currency_code, amount = header_meta[index]

                # Убираем пробелы у значения.
                value = raw_value.strip()

                # Если значение пустое, просто пропускаем его.
                # Это нужно, чтобы отсутствие данных по одной валюте
                # не ломало обработку всей строки.
                if not value:
                    continue

                # Преобразуем строковое значение курса в float.
                rate_value = CNBClient._parse_float(value)

                # Добавляем запись в общий список.
                result.append(
                    ParsedRate(
                        rate_date=rate_date,
                        currency_code=currency_code,
                        country="",
                        currency_name="",
                        amount=amount,
                        rate=rate_value,
                        rate_per_unit=rate_value / amount,
                        source="year.txt",
                    )
                )

        return result

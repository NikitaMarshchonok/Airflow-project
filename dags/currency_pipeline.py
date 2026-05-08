# Это стандартные инструменты Python
from datetime import timedelta
import pendulum   # работа с датами и временем (умнее стандартного datetime)
import requests   # делать HTTP запросы к сайтам и API
import psycopg2   # подключаться к PostgreSQL из Python

# Это инструменты самого Airflow
from airflow.decorators import dag, task


# ───────────────────────────────────────────
# БЛОК 1: настройки по умолчанию для задач
# ───────────────────────────────────────────
# Эти настройки применяются к каждой задаче в DAG
# Если задача упала — Airflow подождёт 5 минут и попробует ещё раз
# Максимум 3 попытки
default_args = {
    "owner": "data_team",          # кто отвечает за этот DAG
    "retries": 3,                  # сколько раз повторить при ошибке
    "retry_delay": timedelta(minutes=5),  # пауза между попытками
}


# ───────────────────────────────────────────
# БЛОК 2: объявляем сам DAG
# ───────────────────────────────────────────
# @dag — это "декоратор". Он говорит Airflow:
# "следующая функция — это не просто функция, это DAG"
@dag(
    dag_id="currency_rates_pipeline",       # имя в интерфейсе Airflow
    schedule="0 * * * *",                  # каждый час (в :00 минут)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # с какой даты считать
    catchup=False,      # НЕ запускать пропущенные часы при первом старте
    default_args=default_args,
    tags=["finance", "etl"],               # теги для фильтрации в UI
)
def currency_pipeline():
    # Внутри этой функции мы объявляем задачи


    # ───────────────────────────────────────────
    # ЗАДАЧА 1: extract — скачиваем данные
    # ───────────────────────────────────────────
    # @task говорит Airflow: "это отдельная задача в графе"
    @task()
    def extract() -> dict:
        # Бесплатный API который отдаёт курсы валют
        url = "https://open.er-api.com/v6/latest/USD"

        # Делаем HTTP GET запрос — как браузер открывает сайт
        # timeout=10 означает: если нет ответа 10 секунд — считаем ошибкой
        response = requests.get(url, timeout=10)

        # Если API вернул ошибку (например 500) — выбросим исключение
        # Airflow поймает его и сделает retry
        response.raise_for_status()

        # Превращаем JSON ответ в словарь Python
        data = response.json()

        # Возвращаем только то что нам нужно
        # pendulum.now("UTC") — текущее время в UTC
        return {
            "rates": data["rates"],
            "fetched_at": pendulum.now("UTC").isoformat(),
        }
        # Это значение автоматически передастся в следующую задачу


    # ───────────────────────────────────────────
    # ЗАДАЧА 2: transform — приводим данные к нужному виду
    # ───────────────────────────────────────────
    # raw_data — это то что вернула задача extract (словарь выше)
    @task()
    def transform(raw_data: dict) -> list:
        # Нас интересуют только эти валюты
        targets = ["EUR", "GBP", "JPY", "ILS", "CHF"]
        records = []  # пустой список, будем его заполнять

        for target in targets:
            # Берём курс для этой валюты из словаря
            rate = raw_data["rates"].get(target)

            # Если данных нет или они странные — пропускаем
            if not isinstance(rate, (int, float)) or rate <= 0:
                continue

            # Добавляем готовую запись в список
            records.append({
                "base": "USD",
                "target": target,
                "rate": round(rate, 6),   # округляем до 6 знаков
                "fetched_at": raw_data["fetched_at"],
            })

        # Если список пустой — что-то пошло не так, бросаем ошибку
        if not records:
            raise ValueError("Нет данных после трансформации!")

        return records  # список словарей уйдёт в задачу load


    # ───────────────────────────────────────────
    # ЗАДАЧА 3: load — сохраняем в базу данных
    # ───────────────────────────────────────────
    # records — это список словарей из задачи transform
    @task()
    def load(records: list) -> int:
        # Подключаемся к PostgreSQL
        # host="postgres" — это имя сервиса из docker-compose.yml
        conn = psycopg2.connect(
            host="postgres",
            database="pipeline_db",
            user="airflow",
            password="airflow",
            port=5432,
        )

        inserted = 0  # счётчик вставленных строк

        # "with conn:" — если что-то упадёт внутри,
        # изменения автоматически откатятся (rollback)
        with conn:
            with conn.cursor() as cur:
                for record in records:
                    # %(name)s — безопасная вставка значений
                    # НЕ делай так: f"INSERT ... VALUES ({record['rate']})"
                    # Это защита от SQL-инъекций
                    cur.execute(
                        """
                        INSERT INTO exchange_rates
                            (base, target, rate, fetched_at)
                        VALUES
                            (%(base)s, %(target)s, %(rate)s, %(fetched_at)s)
                        """,
                        record,  # передаём словарь — psycopg2 подставит значения
                    )
                    inserted += 1

        conn.close()
        # print() попадает в логи Airflow — удобно для отладки
        print(f"Готово! Записано {inserted} курсов валют")
        return inserted


    # ───────────────────────────────────────────
    # СВЯЗЫВАЕМ ЗАДАЧИ: вот и есть наш DAG
    # ───────────────────────────────────────────
    # Airflow смотрит на эти строки и понимает порядок:
    # сначала extract, потом transform, потом load
    raw_data = extract()          # запускаем задачу 1
    clean_data = transform(raw_data)  # передаём результат в задачу 2
    load(clean_data)              # передаём результат в задачу 3


# Эта строка ОБЯЗАТЕЛЬНА — без неё Airflow не увидит DAG
dag_instance = currency_pipeline()
import sqlite3
import asyncio
import time
from datetime import datetime
import logging
import platform
from logging.handlers import SysLogHandler
from ElevationAnalyzer import ElevationAnalyzer
from ma import MovingAverage

DISTANCE = 200
DELAY_MS = 150
WINDOW_SIZE = 5
SMOOTHING = 11
ALPHA = 0.9

# Настройка корневого логгера
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

if platform.system() == 'Windows':
    # Логгер для Windows (в файл)
    file_handler = logging.FileHandler('area-manager.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
    root_logger.addHandler(file_handler)
else:
    # Логгер для Linux (в syslog через systemd)
    syslog_handler = SysLogHandler(address='/dev/log')
    syslog_handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))
    root_logger.addHandler(syslog_handler)

# Логгер для main.py
logger = logging.getLogger('area-manager.main')


ma = MovingAverage(WINDOW_SIZE)
def check_topic_conditions(topic_id, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()

    # Получаем данные топика
    cursor.execute("SELECT Altitude_Topic FROM Topics WHERE ID_Topic = ?", (topic_id,))
    alt = cursor.fetchone()
    if alt is None:
        logger.warning(f"Topic with ID {topic_id} not found.")
        conn.close()
        return False, None  # Топик не найден
    alt = alt[0]
    logger.info(f"Altitude for topic {topic_id}: {alt}")

    # Получаем данные Data для топика
    cursor.execute("SELECT Value_Data, Time_Data FROM Data WHERE ID_Topic = ? ORDER BY Time_Data ASC", (topic_id,))
    data = [{'Value_Data': row[0], 'Time_Data': datetime.fromtimestamp(row[1] / 1000)} for row in cursor.fetchall()]
    conn.close()

    if not data:
        logger.warning(f"No data found for topic {topic_id}.")
        return False, None  # Данные по топику отсутствуют

    logger.info(f"Data for topic {topic_id}: {data}")

    # Вычисляем скользящее среднее и предсказываем 3 события
    #predicted_events = ma.calculate_moving_average(data)
    #predicted_events = ma.calculate_ema_alpha(data, 0.9)
    predicted_events = ma.calculate_ema_smooth(data, SMOOTHING)
    if len(predicted_events) < 10:
        logger.warning(f"Not enough data to predict for topic {topic_id}.")
        return False, None  # Недостаточно данных для предсказания

    p1, p2, p3 = [event['Value_Data'] for event in predicted_events[-3:]]
    logger.info(f"Predicted values for topic {topic_id}: p1={p1}, p2={p2}, p3={p3}")

    # УБРАТЬ
    strP = ""
    for d in data:
        strP = strP + str(d['Value_Data']) + "|"
    logger.info(f"strP: {strP}")
    strP = ""
    for event in predicted_events:
        strP = strP + str(event['Value_Data']) + "|"
    logger.info(f"strP: {strP}")
    # УБРАТЬ

    # Определяем последнюю и предпоследнюю фактическую высоту топика
    if len(data) < 2:
        logger.warning(f"Not enough data to compare for topic {topic_id}.")
        return False, p3  # Недостаточно данных для сравнения

    revertData = data[::-1]
    f1, f2 = float(revertData[0]['Value_Data']), float(revertData[1]['Value_Data'])
    logger.info(f"Actual values for topic {topic_id}: f1={f1}, f2={f2}")

    # Проверяем условия
    if p3 > alt and f1 > f2:
        logger.info(f"Conditions met for topic {topic_id}: p3={p3} > alt={alt} and f1={f1} > f2={f2}")
        return True, p3
    else:
        logger.info(f"Conditions not met for topic {topic_id}: p3={p3} > alt={alt} and f1={f1} > f2={f2}")
        return False, p3

def main():
    logger.info(f"Starting...")
    db_path = '../MQTT_Data_collector/mqtt_data.db'
    analyzer = ElevationAnalyzer(DELAY_MS)

    # Глобальный словарь для хранения времени последнего изменения данных для каждого топика
    last_data_change = {}

    logger.info(f"All done!")
    while True:
        # Получаем все топики
        with sqlite3.connect(db_path) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            cursor = conn.cursor()
            cursor.execute("SELECT ID_Topic, Latitude_Topic, Longitude_Topic, CheckTime_Topic FROM Topics")
            topics = cursor.fetchall()

        for topic in topics:
            topic_id, latitude, longitude, check_time = topic
            logger.info(f"Checking topic {topic_id}")

            # Проверяем, когда был последний расчет
            if check_time is None or (datetime.now() - datetime.fromtimestamp(check_time)).total_seconds() >= 2 * 3600:
                # Если расчет был 2 часа назад, то нужно повторить проверку по параметрам затопления

                # Проверяем, есть ли новые данные в таблице Data с момента последнего расчета
                with sqlite3.connect(db_path) as conn:
                    conn.execute('PRAGMA journal_mode=WAL')
                    cursor = conn.cursor()
                    cursor.execute("SELECT MAX(Time_Data) FROM Data WHERE ID_Topic = ?", (topic_id,))
                    latest_data_time = cursor.fetchone()[0]

                # Если есть новые данные, или это первый расчет для топика
                if (topic_id not in last_data_change) or (latest_data_time is None) or (latest_data_time > last_data_change[topic_id]):
                    last_data_change[topic_id] = latest_data_time  # Обновляем время последнего изменения данных

                    # Открываем соединение для проверки условий
                    with sqlite3.connect(db_path) as conn:
                        conn.execute('PRAGMA journal_mode=WAL')
                        cursor = conn.cursor()
                        conditions_met, p3 = check_topic_conditions(topic_id, db_path)

                    if conditions_met:
                        # Если данные прошли проверку по параметрам затопления, то топику угрожает затопление. Рассчет области затопления.
                        logger.info(f"Conditions met for topic {topic_id}. Calculating area points.")

                        center_coords = (latitude, longitude)
                        initial_height = p3  # Используем последнее предсказанное значение (p3)

                        # Вычисляем точки
                        result = analyzer.find_depression_area_with_islands(center_coords, initial_height, DISTANCE)

                        # Открываем соединение для записи данных
                        with sqlite3.connect(db_path) as conn:
                            conn.execute('PRAGMA journal_mode=WAL')
                            cursor = conn.cursor()

                            # Удаляем старые данные для топика
                            cursor.execute("""
                                DELETE FROM AreaPoints WHERE ID_Topic = ?
                            """, (topic_id,))

                            # Записываем новые данные в таблицу AreaPoints
                            cursor.execute("""
                                INSERT INTO AreaPoints (ID_Topic, Depression_AreaPoint, Perimeter_AreaPoint, Included_AreaPoint, Islands_AreaPoint)
                                VALUES (?, ?, ?, ?, ?)
                            """, (topic_id, str(result['depression_points']), str(result['perimeter_points']),
                                  str(result['included_points']), str(result['islands'])))

                            # Обновляем CheckTime_Topic
                            cursor.execute("""
                                UPDATE Topics SET CheckTime_Topic = ? WHERE ID_Topic = ?
                            """, (datetime.now().timestamp(), topic_id))

                            conn.commit()
                            logger.info(f"Data for topic {topic_id} inserted into AreaPoints and CheckTime_Topic updated.")
                    else:
                        # Если данные не прошли проверку по параметрам затопления, то топику не угрожает затопление. Очистка данных области затопления.
                        logger.info(f"Conditions not met for topic {topic_id}. Clearing data from AreaPoints.")

                        # Открываем соединение для очистки данных
                        with sqlite3.connect(db_path) as conn:
                            conn.execute('PRAGMA journal_mode=WAL')
                            cursor = conn.cursor()

                            # Удаляем данные из таблицы AreaPoints для топика
                            cursor.execute("""
                                DELETE FROM AreaPoints WHERE ID_Topic = ?
                            """, (topic_id,))

                            # Обновляем CheckTime_Topic
                            cursor.execute("""
                                UPDATE Topics SET CheckTime_Topic = ? WHERE ID_Topic = ?
                            """, (datetime.now().timestamp(), topic_id))

                            conn.commit()
                            logger.info(f"Data for topic {topic_id} cleared from AreaPoints and CheckTime_Topic updated.")
                else:
                    # Если новых данных нет, то расчет не требуется, но обновляем CheckTime_Topic, чтобы отметить, что топик был проверен
                    logger.info(f"No new data for topic {topic_id} since last calculation. Updating CheckTime_Topic.")

                    # Открываем соединение для обновления CheckTime_Topic
                    with sqlite3.connect(db_path) as conn:
                        conn.execute('PRAGMA journal_mode=WAL')
                        cursor = conn.cursor()

                        # Обновляем CheckTime_Topic
                        cursor.execute("""
                            UPDATE Topics SET CheckTime_Topic = ? WHERE ID_Topic = ?
                        """, (datetime.now().timestamp(), topic_id))

                        conn.commit()
                        logger.info(f"CheckTime_Topic updated for topic {topic_id}.")
            else:
                # Если топик был проверен менее 2 часов назад, то пропускаем расчет
                logger.info(f"Topic {topic_id} was recently checked. Skipping calculation.")

        time.sleep(60)  # Пауза перед следующей итерацией

# def main():
#     # Пример данных
#     data = [
#         {'Value_Data': '87', 'Time_Data': datetime(2023, 10, 1, 12, 0)},
#         {'Value_Data': '88', 'Time_Data': datetime(2023, 10, 2, 12, 0)},
#         {'Value_Data': '85', 'Time_Data': datetime(2023, 10, 3, 12, 0)},
#         {'Value_Data': '90', 'Time_Data': datetime(2023, 10, 4, 12, 0)},
#         {'Value_Data': '88', 'Time_Data': datetime(2023, 10, 5, 12, 0)},
#         {'Value_Data': '88', 'Time_Data': datetime(2023, 10, 6, 12, 0)},
#         {'Value_Data': '89', 'Time_Data': datetime(2023, 10, 7, 12, 0)},
#         {'Value_Data': '90', 'Time_Data': datetime(2023, 10, 8, 12, 0)},
#         {'Value_Data': '91', 'Time_Data': datetime(2023, 10, 9, 12, 0)},
#         {'Value_Data': '91.5', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '92', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '93', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '94', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '95', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '97', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '98', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '99', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '100', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '102', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '103', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '104', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#         {'Value_Data': '105', 'Time_Data': datetime(2023, 10, 10, 12, 0)},
#     ]
#
#     # Рассчитываем EMA
#     ema_result = calculate_ema(data)
#
#     # Выводим результаты
#     for item in ema_result:
#         print(f"Value_Data: {item['Value_Data']}, Time_Data: {item['Time_Data']}")

# Запуск функции
main()

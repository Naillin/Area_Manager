import sqlite3
import asyncio
import time
from datetime import datetime, timedelta
import logging
import platform
from logging.handlers import SysLogHandler
from ElevationAnalyzer import ElevationAnalyzer

# Настройка логгера
logger = logging.getLogger('area-manager.main')
logger.setLevel(logging.INFO)

if platform.system() == 'Windows':
    # Логгер для Windows (в файл)
    file_handler = logging.FileHandler('area-manager.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
    logger.addHandler(file_handler)
else:
    # Логгер для Linux (в syslog через systemd)
    syslog_handler = SysLogHandler(address='/dev/log')
    syslog_handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))
    logger.addHandler(syslog_handler)

def calculate_moving_average(data, window_size = 7):
    moving_average = []

    # Преобразуем строковые значения в числа
    data_values = [float(item['Value_Data']) for item in data]
    logger.debug(f"Data values: {data_values}")

    # Рассчитываем скользящее среднее
    for i in range(len(data_values)):
        if i < window_size - 1:
            moving_average.append({'Value_Data': None, 'Time_Data': data[i]['Time_Data']})  # Недостаточно данных для расчета
        else:
            sum_values = sum(data_values[i - window_size + 1:i + 1])
            average = sum_values / window_size
            moving_average.append({'Value_Data': average, 'Time_Data': data[i]['Time_Data']})
            logger.debug(f"Moving average at index {i}: {average}")

    # Предсказываем на 3 дня вперед с учетом тенденции
    last_values = data_values[-window_size:]
    last_times = [item['Time_Data'] for item in data[-window_size:]]
    last_average = sum(last_values) / window_size
    logger.debug(f"Last average: {last_average}")

    # Рассчитываем средний интервал времени между последними событиями
    time_intervals = [(last_times[i + 1] - last_times[i]).total_seconds() for i in range(len(last_times) - 1)]
    average_time_interval = sum(time_intervals) / len(time_intervals)
    logger.debug(f"Average time interval: {average_time_interval}")

    # Рассчитываем тенденцию (наклон)
    slope = (last_values[-1] - last_values[0]) / (window_size - 1)
    logger.debug(f"Slope: {slope}")

    for i in range(3):
        predicted_value = last_average + slope * (i + 1)
        predicted_time = last_times[-1] + timedelta(seconds=average_time_interval * (i + 1))
        moving_average.append({'Value_Data': predicted_value, 'Time_Data': predicted_time})
        logger.debug(f"Predicted value for day {i + 1}: {predicted_value} at {predicted_time}")

    return moving_average

def calculate_ema(data, window_size = 7, alpha = 0.5):
    ema = []

    # Преобразуем строковые значения в числа
    data_values = [float(item['Value_Data']) for item in data]

    # Рассчитываем EMA
    for i in range(len(data_values)):
        if i < window_size - 1:
            ema.append({'Value_Data': None, 'Time_Data': data[i]['Time_Data']})  # Недостаточно данных для расчета
        else:
            if i == window_size - 1:
                # Начальное значение EMA - простое скользящее среднее
                sum_values = sum(data_values[0:window_size])
                average = sum_values / window_size
                ema.append({'Value_Data': average, 'Time_Data': data[i]['Time_Data']})
            else:
                # Рассчитываем EMA
                previous_ema = ema[i - 1]['Value_Data']
                current_value = data_values[i]
                current_ema = alpha * current_value + (1 - alpha) * previous_ema
                ema.append({'Value_Data': current_ema, 'Time_Data': data[i]['Time_Data']})

    # Предсказываем на 3 дня вперед с учетом тенденции
    last_values = data_values[-window_size:]
    last_times = [item['Time_Data'] for item in data[-window_size:]]
    last_ema = ema[-1]['Value_Data']

    # Рассчитываем средний интервал времени между последними событиями
    time_intervals = [(last_times[i + 1] - last_times[i]).total_seconds() for i in range(len(last_times) - 1)]
    average_time_interval = sum(time_intervals) / len(time_intervals)

    # Рассчитываем тенденцию (наклон)
    slope = (last_values[-1] - last_values[0]) / (window_size - 1)

    for i in range(3):
        predicted_value = last_ema + slope * (i + 1)
        predicted_time = last_times[-1] + timedelta(seconds=average_time_interval * (i + 1))
        ema.append({'Value_Data': predicted_value, 'Time_Data': predicted_time})

    return ema

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
    cursor.execute("SELECT Value_Data, Time_Data FROM Data WHERE ID_Topic = ? ORDER BY Time_Data DESC", (topic_id,))
    data = [{'Value_Data': row[0], 'Time_Data': datetime.fromtimestamp(row[1] / 1000)} for row in cursor.fetchall()]
    conn.close()

    if not data:
        logger.warning(f"No data found for topic {topic_id}.")
        return False, None  # Данные по топику отсутствуют

    logger.info(f"Data for topic {topic_id}: {data}")

    # Вычисляем скользящее среднее и предсказываем 3 события
    #predicted_events = calculate_moving_average(data, 7)
    predicted_events = calculate_ema(data, 7, 0.9)
    if len(predicted_events) < 10:
        logger.warning(f"Not enough data to predict for topic {topic_id}.")
        return False, None  # Недостаточно данных для предсказания

    p1, p2, p3 = [event['Value_Data'] for event in predicted_events[-3:]]
    logger.info(f"Predicted values for topic {topic_id}: p1={p1}, p2={p2}, p3={p3}")

    strP = ""
    for event in predicted_events:
        strP += str(event['Value_Data'])

    # Определяем последнюю и предпоследнюю фактическую высоту топика
    if len(data) < 2:
        logger.warning(f"Not enough data to compare for topic {topic_id}.")
        return False, p3  # Недостаточно данных для сравнения

    f1, f2 = float(data[0]['Value_Data']), float(data[1]['Value_Data'])
    logger.info(f"Actual values for topic {topic_id}: f1={f1}, f2={f2}")

    # Проверяем условия
    if p3 > alt and f1 < f2:
        logger.info(f"Conditions met for topic {topic_id}: p3={p3} > alt={alt} and f1={f1} < f2={f2}")
        return True, p3
    else:
        logger.info(f"Conditions not met for topic {topic_id}: p3={p3} > alt={alt} and f1={f1} < f2={f2}")
        return False, p3

def main():
    logger.info(f"Starting...")
    db_path = '../MQTT_Data_collector/mqtt_data.db'
    analyzer = ElevationAnalyzer(150)

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
            if check_time is None or (datetime.now() - datetime.fromisoformat(check_time)).total_seconds() >= 2 * 3600:
                # Открываем соединение для проверки условий
                with sqlite3.connect(db_path) as conn:
                    conn.execute('PRAGMA journal_mode=WAL')
                    cursor = conn.cursor()
                    conditions_met, p3 = check_topic_conditions(topic_id, db_path)

                if conditions_met:
                    logger.info(f"Conditions met for topic {topic_id}. Calculating area points.")

                    center_coords = (latitude, longitude)
                    initial_height = p3  # Используем последнее предсказанное значение (p3)

                    # Вычисляем точки
                    result = analyzer.find_depression_area_with_islands(center_coords, initial_height)

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
                            UPDATE Topic SET CheckTime_Topic = ? WHERE ID_Topic = ?
                        """, (datetime.now().timestamp(), topic_id))

                        conn.commit()
                        logger.info(f"Data for topic {topic_id} inserted into AreaPoints and CheckTime_Topic updated.")
                else:
                    logger.info(f"Conditions not met for topic {topic_id}. Skipping calculation.")
            else:
                logger.info(f"Topic {topic_id} was recently checked. Skipping calculation.")

        time.sleep(60)  # Пауза перед следующей итерацией

# Запуск функции
main()

# async def main(): # убрать асинхронность
#     db_path = '../MQTT_Data_collector/mqtt_data.db'
#     analyzer = ElevationAnalyzer()
#
#     while True:
#         # Получаем все топики
#         with sqlite3.connect(db_path) as conn:
#             conn.execute('PRAGMA journal_mode=WAL')
#             cursor = conn.cursor()
#             cursor.execute("SELECT ID_Topic, Latitude_Topic, Longitude_Topic, CheckTime_Topic FROM Topic")
#             topics = cursor.fetchall()
#
#         for topic in topics:
#             topic_id, latitude, longitude, check_time = topic
#             logger.info(f"Checking topic {topic_id}")
#
#             # Проверяем, когда был последний расчет
#             if check_time is None or (datetime.now() - datetime.fromisoformat(check_time)).total_seconds() >= 2 * 3600:
#                 # Открываем соединение для проверки условий
#                 with sqlite3.connect(db_path) as conn:
#                     conn.execute('PRAGMA journal_mode=WAL')
#                     cursor = conn.cursor()
#                     conditions_met, p3 = check_topic_conditions(topic_id, db_path)
#
#                 if conditions_met:
#                     logger.info(f"Conditions met for topic {topic_id}. Calculating area points.")
#
#                     center_coords = (latitude, longitude)
#                     initial_height = p3  # Используем последнее предсказанное значение (p3)
#
#                     # Вычисляем точки
#                     result = await analyzer.find_depression_area_with_islands(center_coords, initial_height)
#
#                     # Открываем соединение для записи данных
#                     with sqlite3.connect(db_path) as conn:
#                         conn.execute('PRAGMA journal_mode=WAL')
#                         cursor = conn.cursor()
#
#                         # Удаляем старые данные для топика
#                         cursor.execute("""
#                             DELETE FROM AreaPoints WHERE ID_Topic = ?
#                         """, (topic_id,))
#
#                         # Записываем новые данные в таблицу AreaPoints
#                         cursor.execute("""
#                             INSERT INTO AreaPoints (ID_Topic, Depression_AreaPoint, Perimeter_AreaPoint, Included_AreaPoint, Islands_AreaPoint)
#                             VALUES (?, ?, ?, ?, ?)
#                         """, (topic_id, str(result['depression_points']), str(result['perimeter_points']),
#                               str(result['included_points']), str(result['islands']))) #понять как работае
#
#                         # Обновляем CheckTime_Topic
#                         cursor.execute("""
#                             UPDATE Topic SET CheckTime_Topic = ? WHERE ID_Topic = ?
#                         """, (datetime.now().timestamp(), topic_id))
#
#                         conn.commit()
#                         logger.info(f"Data for topic {topic_id} inserted into AreaPoints and CheckTime_Topic updated.")
#                 else:
#                     logger.info(f"Conditions not met for topic {topic_id}. Skipping calculation.")
#             else:
#                 logger.info(f"Topic {topic_id} was recently checked. Skipping calculation.")
#
#         await asyncio.sleep(60)  # Пауза перед следующей итерацией
#
# # Запуск асинхронной функции
# asyncio.run(main())

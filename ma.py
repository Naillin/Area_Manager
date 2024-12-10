import logging
from datetime import timedelta

# Инициализация логгера для модуля MovingAverage
logger = logging.getLogger('area-manager.MovingAverage')

class MovingAverage:
    def __init__(self, window_size):
        self.window_size = window_size

    def calculate_moving_average(self, data):
        moving_average = []
        # Преобразуем строковые значения в числа
        data_values = [float(item['Value_Data']) for item in data]
        logger.debug(f"Data values: {data_values}")

        # Рассчитываем скользящее среднее
        for i in range(len(data_values)):
            if i < self.window_size - 1:
                moving_average.append(
                    {'Value_Data': None, 'Time_Data': data[i]['Time_Data']})  # Недостаточно данных для расчета
            else:
                sum_values = sum(data_values[i - self.window_size + 1:i + 1])
                average = sum_values / self.window_size
                moving_average.append({'Value_Data': average, 'Time_Data': data[i]['Time_Data']})
                logger.debug(f"Moving average at index {i}: {average}")

        # Предсказываем на 3 дня вперед с учетом тенденции
        last_values = data_values[-self.window_size:]
        last_times = [item['Time_Data'] for item in data[-self.window_size:]]
        last_average = sum(last_values) / self.window_size
        logger.debug(f"Last average: {last_average}")

        # Рассчитываем средний интервал времени между последними событиями
        time_intervals = [(last_times[i + 1] - last_times[i]).total_seconds() for i in range(len(last_times) - 1)]
        average_time_interval = sum(time_intervals) / len(time_intervals)
        logger.debug(f"Average time interval: {average_time_interval}")

        # Рассчитываем тенденцию (наклон)
        slope = (last_values[-1] - last_values[0]) / (self.window_size - 1)
        logger.debug(f"Slope: {slope}")

        for i in range(3):
            predicted_value = last_average + slope * (i + 1)
            predicted_time = last_times[-1] + timedelta(seconds=average_time_interval * (i + 1))
            moving_average.append({'Value_Data': predicted_value, 'Time_Data': predicted_time})
            logger.debug(f"Predicted value for day {i + 1}: {predicted_value} at {predicted_time}")

        return moving_average

    def calculate_ema_alpha(self, data, alpha=0.2):
        ema = []

        # Преобразуем строковые значения в числа
        data_values = [float(item['Value_Data']) for item in data]

        # Рассчитываем EMA
        for i in range(len(data_values)):
            if i < self.window_size - 1:
                ema.append({'Value_Data': None, 'Time_Data': data[i]['Time_Data']})  # Недостаточно данных для расчета
            else:
                if i == self.window_size - 1:
                    # Начальное значение EMA - простое скользящее среднее
                    sum_values = sum(data_values[0:self.window_size])
                    average = sum_values / self.window_size
                    ema.append({'Value_Data': average, 'Time_Data': data[i]['Time_Data']})
                else:
                    # Рассчитываем EMA
                    previous_ema = ema[i - 1]['Value_Data']
                    current_value = data_values[i]
                    current_ema = alpha * current_value + (1 - alpha) * previous_ema
                    ema.append({'Value_Data': current_ema, 'Time_Data': data[i]['Time_Data']})

        # Предсказываем на 3 дня вперед с учетом тенденции
        last_values = data_values[-self.window_size:]
        last_times = [item['Time_Data'] for item in data[-self.window_size:]]
        last_ema = ema[-1]['Value_Data']

        # Рассчитываем средний интервал времени между последними событиями
        time_intervals = [(last_times[i + 1] - last_times[i]).total_seconds() for i in range(len(last_times) - 1)]
        average_time_interval = sum(time_intervals) / len(time_intervals)

        # Рассчитываем тенденцию (наклон)
        slope = (last_values[-1] - last_values[0]) / (self.window_size - 1)

        for i in range(3):
            predicted_value = last_ema + slope * (i + 1)
            predicted_time = last_times[-1] + timedelta(seconds=average_time_interval * (i + 1))
            ema.append({'Value_Data': predicted_value, 'Time_Data': predicted_time})

        return ema

    def calculate_ema_smooth(self, data, smoothing=2):
        # Проверка на пустоту данных
        if not data:
            return []

        ema_data = []
        alpha = smoothing / (self.window_size + 1)  # Коэффициент сглаживания

        # Преобразуем строковые значения в числа
        data_values = [float(item["Value_Data"]) for item in data]

        # Инициализация EMA: начальное значение берется как первый элемент
        ema = data_values[0]
        #ema_data.append({"Value_Data": ema, "Time_Data": data[0]["Time_Data"]})

        # Рассчитываем EMA для оставшихся данных
        for i in range(len(data_values)):
            if i < self.window_size - 1:
                ema_data.append(
                    {"Value_Data": None, "Time_Data": data[i]["Time_Data"]})  # Недостаточно данных для расчета
            else:
                ema = alpha * data_values[i] + (1 - alpha) * ema
                ema_data.append({"Value_Data": ema, "Time_Data": data[i]["Time_Data"]})
                logger.debug(f"Moving average at index {i}: {ema}")

        # Предсказываем на 3 дня вперед с учетом тенденции
        last_values = data_values[-self.window_size:]
        last_times = [item["Time_Data"] for item in data[-self.window_size:]]
        last_ema = ema  # Последнее рассчитанное EMA
        logger.debug(f"Last ema: {last_ema}")

        # Рассчитываем средний интервал времени между последними событиями
        time_intervals = [(last_times[i] - last_times[i - 1]).total_seconds() for i in range(1, len(last_times))]
        average_time_interval = timedelta(seconds=sum(time_intervals) / len(time_intervals))
        logger.debug(f"Average time interval: {average_time_interval}")

        # Рассчитываем тенденцию (наклон)
        slope = (last_values[-1] - last_values[0]) / (self.window_size - 1)
        logger.debug(f"Slope: {slope}")

        for i in range(3):
            predicted_value = last_ema + slope * (i + 1)
            predicted_time = last_times[-1] + average_time_interval * (i + 1)
            ema_data.append({"Value_Data": predicted_value, "Time_Data": predicted_time})
            logger.debug(f"Predicted value for day {i + 1}: {predicted_value} at {predicted_time}")

        return ema_data

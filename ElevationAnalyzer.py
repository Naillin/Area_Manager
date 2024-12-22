import logging
import asyncio
import time
import aiohttp
import requests
import math

# Инициализация логгера для модуля ElevationAnalyzer
logger = logging.getLogger('area-manager.ElevationAnalyzer')

class ElevationAnalyzer:

    def __init__(self, delay_ms=1000):
        self.delay_ms = delay_ms

    def get_elevation(self, coords, round_digits=6):
        # Округляем координаты
        rounded_coords = [round(coord, round_digits) for coord in coords]
        url = f"https://api.open-elevation.com/api/v1/lookup?locations={rounded_coords[0]},{rounded_coords[1]}"

        # Максимальное количество попыток
        max_attempts = 3
        attempt = 0

        while attempt < max_attempts:
            # Задержка перед запросом
            time.sleep(self.delay_ms / 1000)

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
            }
            try:
                response = requests.get(url, timeout=10, headers=headers)  # Устанавливаем таймаут для запроса
                response.raise_for_status()  # Выбрасываем исключение, если статус ответа не 200
                data = response.json()

                if data.get('results') and len(data['results']) > 0:
                    elevation = data['results'][0]['elevation']
                    logger.info(f"Высота точки {rounded_coords}: {elevation}")
                    return elevation
                else:
                    logger.warning('No elevation data found for the given coordinates.')
                    return None

            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt < max_attempts:
                    logger.warning(f"Request failed: {e}. Retrying in 5 seconds...")
                    time.sleep(5)  # Задержка перед повторной попыткой
                else:
                    logger.error(f"Request failed after {max_attempts} attempts: {e}")
                    return None

    # async def get_elevation(coords, delay_ms=150):
    #     url = f"https://api.open-elevation.com/api/v1/lookup?locations={coords[0]},{coords[1]}"
    #
    #     # Задержка перед запросом
    #     await asyncio.sleep(delay_ms / 1000)
    #
    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(url) as response:
    #             data = await response.json()
    #
    #             if data.get('results') and len(data['results']) > 0:
    #                 elevation = data['results'][0]['elevation']
    #                 logger.info(f"Высота точки {coords}: {elevation}")
    #                 return elevation
    #             else:
    #                 logger.warning('No elevation data found for the given coordinates.')
    #                 return None

    def format_coords(self, coords):
        return f"{coords[0]:.6f},{coords[1]:.6f}"

    def get_neighbors(self, coords, distance=200):
        lat, lon = coords
        neighbors = []
        for d_lat in range(-1, 2):
            for d_lon in range(-1, 2):
                if d_lat == 0 and d_lon == 0:
                    continue
                new_lat = lat + d_lat * (distance / 111320)
                new_lon = lon + d_lon * (distance / (111320 * math.cos(math.radians(lat))))
                neighbors.append((new_lat, new_lon))
        return neighbors

    def find_depression_area_with_islands(self, center_coords, initial_height, distance=200):
        points_to_check = [{'coords': center_coords, 'height': initial_height}]
        checked_points = set()
        depression_points = set()
        non_flooded_points = set()
        perimeter_points = set()
        included_points = set()
        islands = []
        island_id = 0

        def process_point(current_point, current_height):
            current_key = self.format_coords(current_point)
            if current_key in checked_points:
                return
            checked_points.add(current_key)

            current_elevation = self.get_elevation(current_point, 6)

            if current_elevation < current_height:
                depression_points.add(current_key)
                neighbors = self.get_neighbors(current_point)
                for neighbor in neighbors:
                    neighbor_key = self.format_coords(neighbor)
                    if neighbor_key not in checked_points:
                        points_to_check.append({
                            'coords': neighbor,
                            'height': min(current_height, current_elevation)
                        })
            else:
                non_flooded_points.add(current_key)

        while points_to_check:
            for point in points_to_check:
                process_point(point['coords'], point['height'])
            points_to_check = points_to_check[1:]

        for point_key in depression_points:
            lat, lon = map(float, point_key.split(','))
            neighbors = self.get_neighbors((lat, lon))

            has_non_flooded_neighbor = False

            for neighbor in neighbors:
                neighbor_key = self.format_coords(neighbor)

                if neighbor_key not in depression_points:
                    has_non_flooded_neighbor = True

                    if neighbor_key in non_flooded_points:
                        perimeter_points.add(neighbor_key)
                    else:
                        included_points.add(point_key)

            if not has_non_flooded_neighbor:
                existing_island = next((island for island in islands if
                                        any(self.are_neighbors(coord, (lat, lon), 50) for coord in island['coords'])),
                                       None)

                if existing_island:
                    existing_island['coords'].append((lat, lon))
                else:
                    islands.append({'id': island_id + 1, 'coords': [(lat, lon)]})
                    island_id += 1

        logger.info("Depression Points: %s", [list(map(float, point.split(','))) for point in depression_points])
        logger.info("Perimeter Points: %s", [list(map(float, point.split(','))) for point in perimeter_points])
        logger.info("Included Points: %s", [list(map(float, point.split(','))) for point in included_points])
        logger.info("Islands: %s", islands)
        return {
            'depression_points': [list(map(float, point.split(','))) for point in depression_points],
            'perimeter_points': [list(map(float, point.split(','))) for point in perimeter_points],
            'included_points': [list(map(float, point.split(','))) for point in included_points],
            'islands': islands
        }

    # async def find_depression_area_with_islands(self, center_coords, initial_height, distance=200):
    #     points_to_check = [{'coords': center_coords, 'height': initial_height}]
    #     checked_points = set()
    #     depression_points = set()
    #     non_flooded_points = set()
    #     perimeter_points = set()
    #     included_points = set()
    #     islands = []
    #     island_id = 0
    #
    #     async def process_point(current_point, current_height):
    #         current_key = self.format_coords(current_point)
    #         if current_key in checked_points:
    #             return
    #         checked_points.add(current_key)
    #
    #         current_elevation = await self.get_elevation(current_point)
    #
    #         if current_elevation < current_height:
    #             depression_points.add(current_key)
    #             neighbors = self.get_neighbors(current_point)
    #             for neighbor in neighbors:
    #                 neighbor_key = self.format_coords(neighbor)
    #                 if neighbor_key not in checked_points:
    #                     points_to_check.append({
    #                         'coords': neighbor,
    #                         'height': min(current_height, current_elevation)
    #                     })
    #         else:
    #             non_flooded_points.add(current_key)
    #
    #     while points_to_check:
    #         await asyncio.gather(*[process_point(point['coords'], point['height']) for point in points_to_check])
    #         points_to_check = points_to_check[1:]
    #
    #     for point_key in depression_points:
    #         lat, lon = map(float, point_key.split(','))
    #         neighbors = self.get_neighbors((lat, lon))
    #
    #         has_non_flooded_neighbor = False
    #
    #         for neighbor in neighbors:
    #             neighbor_key = self.format_coords(neighbor)
    #
    #             if neighbor_key not in depression_points:
    #                 has_non_flooded_neighbor = True
    #
    #                 if neighbor_key in non_flooded_points:
    #                     perimeter_points.add(neighbor_key)
    #                 else:
    #                     included_points.add(point_key)
    #
    #         if not has_non_flooded_neighbor:
    #             existing_island = next((island for island in islands if any(self.are_neighbors(coord, (lat, lon), 50) for coord in island['coords'])), None)
    #
    #             if existing_island:
    #                 existing_island['coords'].append((lat, lon))
    #             else:
    #                 islands.append({'id': island_id + 1, 'coords': [(lat, lon)]})
    #                 island_id += 1
    #
    #     logger.info("Depression Points: %s", [list(map(float, point.split(','))) for point in depression_points])
    #     logger.info("Perimeter Points: %s", [list(map(float, point.split(','))) for point in perimeter_points])
    #     logger.info("Included Points: %s", [list(map(float, point.split(','))) for point in included_points])
    #     logger.info("Islands: %s", islands)
    #     return {
    #         'depression_points': [list(map(float, point.split(','))) for point in depression_points],
    #         'perimeter_points': [list(map(float, point.split(','))) for point in perimeter_points],
    #         'included_points': [list(map(float, point.split(','))) for point in included_points],
    #         'islands': islands
    #     }

    def are_neighbors(self, coord1, coord2, check_distance=50):
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        distance = math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)
        return distance <= (check_distance / 111320)

import datetime
import os
import time

from src.data_persistence.temperature import Temperature


class DataService:

    SENSOR_TEMPERATURE = 1

    def __init__(self, db_path: str, db_write_interval_ms: int = 1000, db_empty_table_interval_ms: int = 1800000):
        self.db_path = db_path
        self.path_db_temperature = ""
        self.path_config_json = ""
        self.__temperature_records = []
        self.__last_db_write_datetime = datetime.datetime.now()
        self.__db_write_interval_ms = db_write_interval_ms
        self.__last_db_empty_datetime = datetime.datetime.now()
        self.__db_empty_table_interval_ms = db_empty_table_interval_ms
        self.temperature = Temperature(self.db_path)
        self.path_db_temperature = os.path.join(self.db_path, Temperature.DB_NAME)

    def start(self):
        if os.path.exists(self.path_db_temperature):
            os.remove(self.path_db_temperature)
        self._create_db_env()

    def _create_db_env(self):
        res_create_temperature = self.temperature.create_db()
        if not res_create_temperature:
            print("Error to create the data base to support grafana panels")
        else:
            print(f"{Temperature.DB_NAME} database Created\n")

    def get_json_node_data(self, data: dict):
        self.temperature.open_connection()
        sensor_id = data.get("sensor_id")
        if sensor_id == self.SENSOR_TEMPERATURE:
            self._process_temperature_db(data)

        if self._time_to_write():
            self._insert_temperature_db()

        if self._time_to_empty_tables():
            self.temperature.empty_table_traffic()

    def _process_temperature_db(self, data: dict):
            self.__temperature_records.append({"TIME": int(time.time()), "SENSOR_ID": data.get("sensor_id"), "VALUE": data.get("value")})

    def _insert_temperature_db(self):
        res = self.temperature.insert_data_table_temperature(self.__temperature_records)
        if not res:
            print("Not inserted correctly in temperature table db")
        self.__temperature_records.clear()

    def _time_to_write(self) -> bool:
        current_time = datetime.datetime.now()
        delta_time_ms = (current_time - self.__last_db_write_datetime).total_seconds() * 1000
        if delta_time_ms >= self.__db_write_interval_ms:
            self.__last_db_write_datetime = current_time
            return True
        return False

    def _time_to_empty_tables(self):
        current_time = datetime.datetime.now()
        delta_time_ms = (current_time - self.__last_db_empty_datetime).total_seconds() * 1000
        if delta_time_ms >= self.__db_empty_table_interval_ms:
            self.__last_db_empty_datetime = current_time
            return True
        return False
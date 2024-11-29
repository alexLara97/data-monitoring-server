import datetime
import os

from src.data_persistence.temperature import Temperature


class DataService:

    def __init__(self, db_path: str, db_write_interval_ms: int = 1000):
        self.temperature = None
        self.db_path = db_path

        self.path_db_temperature = ""

        self.path_config_json = ""

        self.__temperature_records = list()

        self.__last_db_write_datetime = datetime.datetime.now()
        self.__db_write_interval_ms = db_write_interval_ms
        self.__reset_temperature_db_interval_ms = 1800000  # 30 minutes

    def start(self):
        self.temperature = Temperature(self.db_path)
        self.path_db_temperature = os.path.join(self.db_path, Temperature.DB_NAME)

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
        temperature = []
        try:
            if "Temperature" in data:
                temperature = data["Temperature"]

            if len(temperature) > 0:
                self._process_temperature_db(temperature, data["Timestamp"])

        except Exception as ex:
            print(ex)
        current_datetime = datetime.datetime.now()
        delta_time_ms = (current_datetime - self.__last_db_write_datetime).total_seconds() * 1000.0
        if delta_time_ms >= self.__db_write_interval_ms:
            self.__last_db_write_datetime = current_datetime
            self._insert_temperature_db()

    def _process_temperature_db(self, data: list, time_stamp: int):
        for i in data:
            adjacent = {"TIME": time_stamp,
                        "SENSOR_ID": i["sensor_id"],
                        "VALUE": i["value"],
                        }
            self.__temperature_records.append(adjacent)

    def _insert_temperature_db(self):
        # print("Inserting " + str(len(self.__adjacent_records)) + " registers in adjacent table...")
        res = self.temperature.insert_data_table_temperature(self.__temperature_records)
        if not res:
            print("Not inserted correctly in temperature table db")
        self.__temperature_records = list()
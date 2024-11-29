import os
from typing import List

from src.utils.db.sql import ServiceDB


class Temperature(ServiceDB):
    DB_NAME = "TEMPERATURE.db"

    _table_temperature: str = "TEMPERATURE"
    _list_fields_temperature: list = ["TIME", "SENSOR_ID", "VALUE"]
    _list_fields_type_temperature: list = ["TIMESTAMP", "INTEGER", "REAL"]
    _primary_key_temperature = ""

    POS_TIMESTAMP_temperature: int = 0
    POS_SENSOR_ID_temperature: int = 1
    POS_VALUE_temperature: int = 2


    def __init__(self, db_path=None):
        db_name = self.DB_NAME
        self.db_directory_path = db_path
        super().__init__(db_name=db_name, db_path=self.db_directory_path)

    def start_db(self):
        if not os.path.exists(self.db_directory_path):
            self.create_db()

    def create_db(self) -> bool:
        return self.create_table(self._table_temperature, self._list_fields_temperature, self._list_fields_type_temperature)

    def insert_data_table_temperature(self, records: List[dict]):
        return self.insert_records_db(self._table_temperature, self._list_fields_temperature, records, self._db_connection)

    def get_last_time_temperature(self) -> (bool, int):
        sql: str = f"SELECT MAX({self._list_fields_temperature[self.POS_TIMESTAMP_temperature]}) FROM {self._table_temperature}"
        res, record_list = self._db.query_sql(sql, (), [self._list_fields_temperature[self.POS_TIMESTAMP_temperature]])
        if record_list[0][self._list_fields_temperature[self.POS_TIMESTAMP_temperature]] is not None and len(record_list) > 0:
            return res, record_list[0][self._list_fields_temperature[self.POS_TIMESTAMP_temperature]]
        else:
            return res, 0

    def empty_table_traffic(self) -> bool:
        return self.empty_data_table(self._table_temperature)


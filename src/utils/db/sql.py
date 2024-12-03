import os
import sqlite3
from typing import List


class ServiceDB:

    def __init__(self, db_name: str, db_path: str = None):
        path = db_path

        if not os.path.exists(path):
            os.makedirs(path)
        self.path_db = os.path.join(path, db_name)
        self._db = SqlUtils(self.path_db)

        self._db_connection = None

    def open_connection(self):
        if not self._db_connection:
            self._db_connection = sqlite3.connect(self.path_db)
        if not self._db_connection:
            raise Exception("Error connecting the data base")

    def create_table(self, table_name: str, list_fields: list, list_fields_type: list, primary_key: str = None) -> bool:
        fields: list = []
        for i in range(0, len(list_fields)):
            fields.append(f"{list_fields[i]} {list_fields_type[i]}")
        if primary_key is not None:
            fields.append(f"PRIMARY KEY ({primary_key})")
        sql: str = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)})"
        result: bool = self._db.create_db(sql)
        return result

    @staticmethod
    def validate_record(list_fields: list, record: dict) -> bool:
        if not all(item in list_fields for item in list(record.keys())):
            return False
        else:
            return True

    def insert_record_db(self, table_name: str, list_fields: list, record: dict,
                         connection: sqlite3.Connection = None) -> (bool, int):
        if not self.validate_record(list_fields, record):
            return False, 0

        fields: list = list()
        params: list = list()
        lq: list = list()

        for i in range(0, len(list_fields)):
            fields.append(list_fields[i])
            if record[list_fields[i]] == "NULL":
                lq.append("NULL")
            else:
                params.append(record[list_fields[i]])
                lq.append("?")

        sql: str = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(lq)})"
        if connection is None:
            return self._db.insert_sql(sql, tuple(params))
        else:
            return self._db.execute_sql_tx(sql, tuple(params), connection)

    def insert_records_db(self, table_name: str, list_fields: list, records: List[dict],
                         connection: sqlite3.Connection) -> (bool, int):

        fields: list = list_fields
        params: list = list()
        lq: list = ["?"] * len(list_fields)
        values: list = list()

        for record_item in records:
            if not self.validate_record(list_fields, record_item):
                return False, 0
            values: list = list()
            for field in list_fields:
                if record_item[field] == "NULL":
                    values.append(None)
                else:
                    values.append(record_item[field])
            params.append(tuple(values))

        sql: str = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(lq)})"
        return self._db.execute_sql_tx_many(sql, params, connection)

    def is_empty_table(self, table_name: str) -> bool:

        check, files_number = self._db.files_number_table(table_name)
        if not check:
            return None
        else:
            return files_number == 0

    def empty_data_table(self, table_name: str) -> bool:
        return self._db.empty_data_table(table_name)

    def dell_data_table(self, table_name: str) -> bool:
        return self._db.dell_data_table(table_name)


class SqlUtils:

    def __init__(self, path: str):
        self._p = path

    def create_db(self, sql: str) -> bool:
        check = False
        connection: sqlite3.Connection = None
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            cursor.execute("PRAGMA synchronous=normal;")
            cursor.execute("PRAGMA journal_mode=WAL;")
            connection.commit()
            check = True
        except Exception as ex:
            check = False
        finally:
            if connection:
                connection.close()
        return check

    def insert_sql(self, sql: str, params: tuple):
        connection: sqlite3.Connection = None
        check: bool = False
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
            check = True
        except Exception as ex:
            return check
        finally:
            if connection:
                connection.close()
        return check

    @staticmethod
    def execute_sql_tx(sql: str, params: tuple, connection: sqlite3.Connection):
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            return True, 0
        except Exception as ex:
            return False, 0

    @staticmethod
    def execute_sql_tx_many(sql: str, params: List[tuple], connection: sqlite3.Connection):
        try:
            connection.executemany(sql, params)
            connection.commit()
            return True, 0
        except Exception as ex:
            return False, 0

    def query_sql(self, sql: str, params: tuple, list_field: list) -> (bool, list):
        connection: sqlite3.Connection = None
        check = [False, None]
        list_res: list = []
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(sql, params)
            register = cursor.fetchall()
            for i in register:
                counter = 0
                res = dict()
                for j in list_field:
                    res[j] = i[counter]
                    counter += 1
                list_res.append(res)
            check = [True, list_res]
        except Exception as ex:
            return check[0], check[1]
        finally:
            if connection:
                connection.close()
        return check[0], check[1]

    def files_number_table(self, table_name) -> (bool, int):
        connection: sqlite3.Connection = None
        check = False
        files_number = 0
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            files_number = cursor.fetchone()[0]
            connection.close()
            check = True
        except Exception as ex:
            return check, files_number
        finally:
            if connection:
                connection.close()
        return check, files_number

    def dell_data_table(self, table_name: str) -> bool:

        connection: sqlite3.Connection = None
        check = False
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(f"DROP TABLE {table_name}")
            connection.commit()
            check = True
        except Exception as ex:
            return check
        finally:
            if connection:
                connection.close()
        return check

    def empty_data_table(self, table_name: str) -> bool:

        connection: sqlite3.Connection = None
        check = False
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            connection.commit()
            cursor.execute("VACUUM")
            check = True
        except Exception as ex:
            return check
        finally:
            if connection:
                connection.close()
        return check

    def update_sql(self, sql: str, params: tuple) -> bool:
        connection: sqlite3.Connection = None
        check: bool = False
        try:
            connection = sqlite3.connect(self._p)
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
            check = True
        except Exception as ex:
            return check
        finally:
            if connection:
                connection.close()
        return check
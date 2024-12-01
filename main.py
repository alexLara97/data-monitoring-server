import json
import time

from src.services.communication_service import CommunicationService
from src.services.data_service import DataService

PATH_SETTING_CONFIG_FILE = "./config_files/settings.json"

def main():
    with open(PATH_SETTING_CONFIG_FILE) as setting_file:
        settings = json.load(setting_file)
    ip_target, port_target = settings.get("socket_ip"), settings.get("socket_port")
    db_write_interval_ms = settings.get("db_write_interval_ms")
    db_path_directory = settings.get("db_file_path_directory")
    print("Creating socket communication...\n")
    # Data
    data_service = DataService(db_path=db_path_directory, db_write_interval_ms=db_write_interval_ms)
    data_service.start()
    # Network
    communication_service = CommunicationService(ip_target=ip_target, port_target=port_target,
                                                 data_service=data_service)
    communication_service.start()
    print("Communication successfully created in {}:{}\n".format(ip_target, port_target))
    time.sleep(0.5)
    while True:
        enter = input("Press ENTER to finish\n")
        if enter == "":
            break
    communication_service.stop()
    time.sleep(0.2)
    print("Closed communication")
    time.sleep(1)

if __name__ == "__main__":
    main()
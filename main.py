import json
import time

PATH_SETTING_CONFIG_FILE = "./config_files/setting.json"

def main():
    with open(PATH_SETTING_CONFIG_FILE) as setting_file:
        settings = json.load(setting_file)
    ip_target, port_target = settings.get("socket_ip"), settings.get("socket_port")
    db_path_directory = settings.get("db_file_path_directory")
    print("Creating socket communication...\n")
    # Data
    
    # Network
    print("Communication successfully created in {}:{}\n".format(ip_target, port_target))
    time.sleep(0.5)

if __name__ == "__main__":
    main()
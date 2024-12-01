import queue
from threading import Thread, Event
import json
from src.utils.network_protocol.udp.server import ServerUDP


class CommunicationService:
    THREAD_PROCESS = "thread_db_process"

    def __init__(self, ip_target: str, port_target: int, data_service=None):
        self.ip_communication, self.port_communication, self.data_service = ip_target, port_target, data_service
        self._thread = None
        self.server_udp = None
        self._stop_thread = Event()
        self._callback_fcn = None

    def start(self):
        self._create_socket_server()
        if not self.get_run_status(self._thread):
            self._thread = Thread(target=self._run)
            self._thread.name = f"THREAD_{self._thread}_comm"
            self._thread.daemon = True
            self._thread.start()

    def stop(self):
        self._stop_socket_server()
        # Stop the thread
        self._stop_thread.set()

    def need_stop(self) -> bool:
        return self._stop_thread.is_set()

    @staticmethod
    def get_run_status(thread) -> bool:
        if thread is not None:
            return thread.is_alive()
        else:
            return False

    def callback_data(self, data):
        # db inserts data
        data = data.decode("utf-8")
        data_json = json.loads(data)
        if self.data_service is not None:
            self.data_service.get_json_node_data(data_json)
        else:
            print(data_json)

    def _create_socket_server(self):
        self.server_udp = ServerUDP(self.ip_communication, self.port_communication)
        self.server_udp.initialize()
        self.server_udp.set_callback_function(self.callback_data)

    def _stop_socket_server(self):
        self.server_udp.close_socket()

    def _run(self):
        while not self.need_stop():
            self.server_udp.read_socket_data()

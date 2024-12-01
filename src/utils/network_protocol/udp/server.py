import socket

SOCKET_BUFFER_SIZE = 65536


class ServerUDP:

    def __init__(self, listen_ip: str, connection_port: int):
        self._listen_ip, self._connection_port = listen_ip, connection_port
        self._local_address = (self._listen_ip, self._connection_port)
        self._listen_socket = None
        self._callback_fcn = None

    def initialize(self):
        self._open_socket(self._listen_ip, self._connection_port)

    def close_socket(self):
        self._listen_socket.close()

    def read_socket_data(self):
        data = None
        try:
            data, remote_address = self._listen_socket.recvfrom(SOCKET_BUFFER_SIZE)
            print(data)
            if data:
                self._callback_fcn(data)
        except Exception as ex:
            print(ex)
        return data

    def set_callback_function(self, callback_fcn):
        self._callback_fcn = callback_fcn

    def _open_socket(self, listen_ip, connection_port: int):
        try:
            self._listen_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self._listen_socket.bind((listen_ip, connection_port))
        except Exception as ex:
            print(ex)
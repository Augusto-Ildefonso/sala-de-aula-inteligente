from utils import *
from global_vars import *
import socket
import threading
import time
import json


class Cliente:
    def __init__(self):
        self.gerenciador_addr = None
        self.tcp_socket = None
        self.connected = False

        self.DELAY = 0.1
        self.MAX_RETRIES = 3

        self.discovery_thread = threading.Thread(target=self.__discovery, daemon=True)
        self.discovery_thread.start()

        self.tcp_thread = threading.Thread(target=self.__syn, daemon=True)
        self.tcp_thread.start()

    def __discovery(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_socket.settimeout(5)

        discover_header = Header(0, ("255.255.255.255", 0),
                                 (GERENCIADOR, GERENCIADOR_PORT_UDP),
                                 HEADER_SIZE).to_bytes()

        while not self.connected:
            try:
                udp_socket.sendto(discover_header,
                                  ("255.255.255.255", GERENCIADOR_PORT_UDP))

                data, addr = udp_socket.recvfrom(1024)
                header, _ = parse_payload(data)

                if header and header.tipo_primitiva == 4:
                    self.gerenciador_addr = tuple(header.id_rem.split(":"))
                    self.gerenciador_addr = (self.gerenciador_addr[0],
                                             int(self.gerenciador_addr[1]))
                    break
            except socket.timeout:
                pass
            except Exception:
                time.sleep(1)

        udp_socket.close()

    def __syn(self):
        while not self.gerenciador_addr:
            time.sleep(1)

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(("0.0.0.0", 0))
        self.tcp_socket.connect(self.gerenciador_addr)
        self.cliente_addr = self.tcp_socket.getsockname()

        syn_header = Header(1, self.cliente_addr, self.gerenciador_addr,
                            HEADER_SIZE).to_string()
        syn_data = {"type": 3}
        payload = payload_format(syn_header, json.dumps(syn_data))
        self.tcp_socket.sendall(to_bytes(payload))

        response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
        header, _ = parse_payload(response)

        if header and header.tipo_primitiva == 5:
            self.connected = True

    def request_data(self, date: str) -> bool:
        if not self.connected or not self.tcp_socket:
            return False

        header = Header(9, self.cliente_addr, self.gerenciador_addr,
                        HEADER_SIZE).to_string()
        send_data = {"DATE": date}
        payload = payload_format(header, json.dumps(send_data))

        delay = self.DELAY
        for i in range(self.MAX_RETRIES):
            self.tcp_socket.sendall(to_bytes(payload))

            try:
                self.tcp_socket.settimeout(delay)
                response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
                self.tcp_socket.settimeout(None)
                res_header, res_data = parse_payload(response)

                if res_header and res_header.tipo_primitiva == 8:
                    print(f"[Cliente] Dados recebidos: {res_data['DATA']}")

                    ack_header = Header(10, self.cliente_addr, self.gerenciador_addr,
                                        HEADER_SIZE).to_string()
                    ack_payload = payload_format(ack_header)
                    self.tcp_socket.sendall(to_bytes(ack_payload))
                    return True
            except socket.timeout:
                pass
            except Exception:
                return False

            if i < self.MAX_RETRIES - 1:
                delay *= 2

        return False

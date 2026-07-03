from utils import *
from global_vars import *
import socket
import threading
import time
import json


class Atuador:
    def __init__(self, actuator_type: int, state_shared=None):
        self.actuator_type = actuator_type
        self.nome = ATUADOR_NOME.get(actuator_type, f"Atuador tipo {actuator_type}")

        self.gerenciador_addr = None
        self.tcp_socket = None
        self.connected = False
        self.state = 0
        self.state_shared = state_shared

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
            except Exception as e:
                time.sleep(1)

        udp_socket.close()

    def __syn(self):
        while not self.gerenciador_addr:
            time.sleep(1)

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(("0.0.0.0", 0))
        self.tcp_socket.connect(self.gerenciador_addr)
        self.atuador_addr = self.tcp_socket.getsockname()

        syn_header = Header(1, self.atuador_addr, self.gerenciador_addr,
                            HEADER_SIZE).to_string()
        syn_data = {"type": self.actuator_type}
        payload = payload_format(syn_header, json.dumps(syn_data))
        self.tcp_socket.sendall(to_bytes(payload))

        response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
        header, _ = parse_payload(response)

        if header and header.tipo_primitiva == 5:
            self.connected = True
            self.__listen()
        else:
            print(f"[{self.nome}] Falha na conexao com o gerenciador")

    def __listen(self):
        while self.connected:
            try:
                payload = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
                if not payload:
                    break
                header, data = parse_payload(payload)

                if header and header.tipo_primitiva == 7:
                    command = data["command"]
                    self.__execute_command(command)
            except (ConnectionResetError, ConnectionAbortedError, OSError):
                break
            except Exception:
                break

        self.connected = False

    def __execute_command(self, command: int):
        success = True
        try:
            self.state = command
            if self.state_shared:
                self.state_shared.value = self.state
            state_str = "ligado" if self.state else "desligado"
            print(f"[{self.nome}] {state_str}")
        except Exception:
            success = False

        status = 1 if success else 0
        ack_header = Header(3, self.atuador_addr, self.gerenciador_addr,
                            HEADER_SIZE).to_string()
        ack_data = {"status": status}
        payload = payload_format(ack_header, json.dumps(ack_data))

        delay = self.DELAY
        for i in range(self.MAX_RETRIES):
            try:
                self.tcp_socket.sendall(to_bytes(payload))
                return
            except Exception:
                if i < self.MAX_RETRIES - 1:
                    delay *= 2
                    time.sleep(delay)

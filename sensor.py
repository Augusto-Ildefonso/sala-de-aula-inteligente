from utils import *
from global_vars import *
import socket
import threading
import time
import json


class Sensor:
    def __init__(self, sensor_type: int):
        """Inicializa o sensor e inicia o processo de descoberta do gerenciador.

        Args:
            sensor_type (int): Tipo do sensor (0=presença, 1=leitor, 2=chave).
        """
        self.sensor_type = sensor_type

        # Endereço do gerenciador e socket TCP
        self.gerenciador_addr = None
        self.tcp_socket = None

        # Flags de estado da conexão
        self.connected = False

        # Constantes
        self.DELAY = 0.1
        self.MAX_RETRIES = 3

        # Thread de descoberta UDP
        self.discovery_thread = threading.Thread(target=self.__discovery, daemon=True)
        self.discovery_thread.start()

        # Thread de conexão TCP
        self.tcp_thread = threading.Thread(target=self.__syn, daemon=True)
        self.tcp_thread.start()

    def __discovery(self):
        """Thread responsável por descobrir o gerenciador via broadcast UDP.

        Envia DISCOVER para o broadcast, aguarda resposta OFFER e extrai o
        endereço TCP do gerenciador.
        """
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
                print(f"\nDISCOVER enviado para broadcast:{GERENCIADOR_PORT_UDP}")

                data, addr = udp_socket.recvfrom(1024)
                header, _ = parse_payload(data)

                if header and header.tipo_primitiva == 4:
                    self.gerenciador_addr = tuple(header.id_rem.split(":"))
                    self.gerenciador_addr = (self.gerenciador_addr[0],
                                             int(self.gerenciador_addr[1]))
                    print(f"OFFER recebido de {self.gerenciador_addr}")
                    break
            except socket.timeout:
                print("Timeout no DISCOVER, reenviando...")
            except Exception as e:
                print(f"Erro no DISCOVER: {e}")
                time.sleep(1)

        udp_socket.close()

    def __syn(self):
        """Thread responsável por conectar ao gerenciador via TCP.

        Cria um socket TCP, conecta ao endereço obtido pelo DISCOVER e envia
        SYN com o tipo do sensor, aguardando ACK do gerenciador.
        """
        while not self.gerenciador_addr:
            time.sleep(1)

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(("0.0.0.0", 0))
        self.tcp_socket.connect(self.gerenciador_addr)
        self.sensor_addr = self.tcp_socket.getsockname()
        print(f"\nTCP conectado a {self.gerenciador_addr} "
              f"(sensor em {self.sensor_addr[0]}:{self.sensor_addr[1]})")

        syn_header = Header(1, self.sensor_addr, self.gerenciador_addr,
                            HEADER_SIZE).to_string()
        syn_data = {"type": self.sensor_type}
        payload = payload_format(syn_header, json.dumps(syn_data))
        self.tcp_socket.sendall(to_bytes(payload))
        print(f"SYN enviado para {self.gerenciador_addr}")

        response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
        header, _ = parse_payload(response)

        if header and header.tipo_primitiva == 5:
            self.connected = True
            print(f"ACK recebido de {self.gerenciador_addr} - conexão estabelecida")
        else:
            print("Falha no SYN")

    def send_data(self, data: dict) -> bool:
        """Envia dados para o gerenciador e aguarda ACK_DATA.

        Args:
            data (dict): Dados do sensor conforme o tipo:
                - presença (0): {"EMPTY": bool}
                - leitor (1):    {"NROALUNO": str, "NOME": str}
                - chave (2):     {"STATUS": bool}

        Returns:
            bool: True se ACK_DATA foi recebido, False caso contrário.
        """
        if not self.connected or not self.tcp_socket:
            print("Sensor não conectado ao gerenciador")
            return False

        header = Header(2, self.sensor_addr, self.gerenciador_addr,
                        HEADER_SIZE).to_string()
        payload = payload_format(header, json.dumps(data))

        delay = self.DELAY
        for i in range(self.MAX_RETRIES):
            self.tcp_socket.sendall(to_bytes(payload))
            print(f"\nTentativa {i}: SEND_DATA enviado para {self.gerenciador_addr}")

            try:
                self.tcp_socket.settimeout(delay)
                response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
                self.tcp_socket.settimeout(None)
                res_header, _ = parse_payload(response)

                if res_header and res_header.tipo_primitiva == 6:
                    print(f"ACK_DATA recebido de {self.gerenciador_addr} - sucesso")
                    return True
            except socket.timeout:
                pass
            except Exception as e:
                print(f"Erro no SEND_DATA: {e}")
                return False

            if i < self.MAX_RETRIES - 1:
                delay *= 2

        print(f"SEND_DATA para {self.gerenciador_addr} - falha após "
              f"{self.MAX_RETRIES} tentativas")
        return False

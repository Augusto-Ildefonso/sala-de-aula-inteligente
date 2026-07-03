from utils import *
from global_vars import *
import socket
import threading
import time
import json


class Atuador:
    def __init__(self, actuator_type: int):
        """Inicializa o atuador e inicia o processo de descoberta do gerenciador.

        Args:
            actuator_type (int): Tipo do atuador (4=iluminação, 5=projetor, 6=ar_condicionado).
        """
        self.actuator_type = actuator_type

        # Endereço do gerenciador e socket TCP
        self.gerenciador_addr = None
        self.tcp_socket = None

        # Flags de estado da conexão
        self.connected = False

        # Estado interno do atuador (0=desligado, 1=ligado)
        self.state = 0

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
        SYN com o tipo do atuador, aguardando ACK do gerenciador. Após a
        conexão, inicia o loop de escuta de comandos.
        """
        while not self.gerenciador_addr:
            time.sleep(1)

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(("0.0.0.0", 0))
        self.tcp_socket.connect(self.gerenciador_addr)
        self.atuador_addr = self.tcp_socket.getsockname()
        print(f"\nTCP conectado a {self.gerenciador_addr} "
              f"(atuador em {self.atuador_addr[0]}:{self.atuador_addr[1]})")

        syn_header = Header(1, self.atuador_addr, self.gerenciador_addr,
                            HEADER_SIZE).to_string()
        syn_data = {"type": self.actuator_type}
        payload = payload_format(syn_header, json.dumps(syn_data))
        self.tcp_socket.sendall(to_bytes(payload))
        print(f"SYN enviado para {self.gerenciador_addr}")

        response = self.tcp_socket.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
        header, _ = parse_payload(response)

        if header and header.tipo_primitiva == 5:
            self.connected = True
            print(f"ACK recebido de {self.gerenciador_addr} - conexão estabelecida")
            self.__listen()
        else:
            print("Falha no SYN")

    def __listen(self):
        """Loop de escuta de comandos do gerenciador.

        Permanece recebendo mensagens SEND_COMMAND e respondendo com
        ACK_COMMAND enquanto a conexão estiver ativa.
        """
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
            except Exception as e:
                print(f"Erro no listen: {e}")
                break

        self.connected = False

    def __execute_command(self, command: int):
        """Executa o comando recebido e envia ACK_COMMAND para o gerenciador.

        Args:
            command (int): 0 para desligar, 1 para ligar.
        """
        success = True
        try:
            self.state = command
            state_str = "ligado" if self.state else "desligado"
            print(f"\nComando executado: atuador {self.actuator_type} {state_str}")
        except Exception as e:
            print(f"Erro ao executar comando: {e}")
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
                print(f"ACK_COMMAND enviado para {self.gerenciador_addr} "
                      f"(status: {status})")
                return
            except Exception as e:
                print(f"Erro ao enviar ACK_COMMAND: {e}")
                if i < self.MAX_RETRIES - 1:
                    delay *= 2
                    time.sleep(delay)

        print(f"ACK_COMMAND para {self.gerenciador_addr} - falha após "
              f"{self.MAX_RETRIES} tentativas")

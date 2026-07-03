from socket_manager import create_socket_tcp, create_socket_udp
from utils import *
from global_vars import *
from datetime import date, datetime
import socket
import threading
import time


"""
Mapeamento sensores
0 -> presença
1 -> leitor
2 -> chave

Mapeamento atuadores
4 -> iluminação
5 -> projetor
6 -> ar_condicionado

Padrão do cabeçalho
TIPO_PRIMITIVA|ID_REM|ID_DEST|TAM

Mapeamento TIPO_PRIMITIVA
0 -> DISCOVER
1 -> SYN
2 -> SEND_DATA
3 -> ACK_COMMAND
4 -> OFFER
5 -> ACK
6 -> ACK_DATA
7 -> SEND_COMMAND
8 -> SEND_DATA
9 -> REQUEST_DATA
10 -> ACK_DATA cliente
11 -> não conectado
"""

class Gerenciador:
    def __init__(self):
        # Criando socket UDP (para broadcast)
        self.udp_thread = threading.Thread(target=self.__udp_socket_thread, daemon=True)
        self.udp_thread.start()

        # Criando socket TCP (resto da comunicação)
        self.tcp_thread = threading.Thread(target=self.__tcp_socket_thread, daemon=True)
        self.tcp_thread.start()

        # Dispositivos conectados
        self.connections = {}

        # Respostas de ACK_COMMAND pendentes (addr -> status)
        self.ack_responses = {}

        # Lista de alunos
        self.attendence= {"2026-06-12": [("Augusto", "15441810"), ("Pedro", "11223344")]}

        # Há pessoas na sala
        self.has_students = False

        # Projetor
        self.projector = False

        # Timer para sala vazia (req 3.3)
        self.empty_timer = None

        # Constantes
        self.DELAY = 0.5
        self.MAX_RETRIES = 3



    def __udp_socket_thread(self):
        """Thread responsável pelo funcionamento do socket UDP.

        Ela recebe requisições de broadcast (DISCOVER), processa ela e envia o endereço TCP do gerenciador (OFFER).
        """
        udp_socket = create_socket_udp("0.0.0.0", GERENCIADOR_PORT_UDP)
        while True:
            data, addr = udp_socket.recvfrom(1024)
            print(f"\n[Gerenciador] DISCOVER recebido de {addr[0]}:{addr[1]}")
            data = data.decode()
            header = Header(4, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            udp_socket.sendto(header, addr)
            print(f"[Gerenciador] OFFER enviado para {addr[0]}:{addr[1]}")

    def __ack(self, client : socket.socket, data : dict, addr : tuple[str, str]):
        """Método que que processa requisições do tipo SYN e devolve o ACK.

        Esse método lida com o estabelecimento da conexão e retorna a resposta ACK.

        Args:
            client (socket.socket): Socket do cliente.
            data (dict): JSON com o payload da requisição.
            addr (tuple[str, str]): Tupla com IP e Porta
        """
        print(f"\n[Gerenciador] SYN recebido de {addr[0]}:{addr[1]}")
        if data:
            data_type = int(data["type"])
            tipo_nome = SENSOR_NOME.get(data_type, ATUADOR_NOME.get(data_type, f"tipo {data_type}"))
            self.connections[addr] = ("atuador" if data_type >= 4 else "sensor", data_type, client)
            ack_header_bytes = Header(5, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            client.sendall(ack_header_bytes)
            print(f"[Gerenciador] Conexão estabelecida com {tipo_nome} ({addr[0]}:{addr[1]})")

    def __connection_check(self, addr : tuple[str, str]) -> bool:
        """Método que checa se o addr recebido está conectado.

        Args:
            addr (tuple[str, str]): Tupla com IP e Porta.

        Returns:
            bool: True se estiver conectado e False se não estiver.
        """
        if addr in self.connections.keys():
            return True
        else:
            return False
        
    def __ack_data(self, client : socket.socket, data : dict, addr : tuple[str, str]):
        """Método que processa requisições do tipo SEND_DATA e devolve o ACK_DATA.

        Esse método processa qual tipo de dado que está sendo recebido, chama as respectivas funções e retorna o ACK_DATA.

        Args:
            client (socket.socket): Socket do cliente.
            addr (tuple[str, str]): Tupla que contém o IP e a Porta.
        """
        print(f"\n[Gerenciador] SEND_DATA recebido de {addr[0]}:{addr[1]}")
        if not self.__connection_check(addr):
            header = Header(11, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            client.sendall(header)
            return

        header = Header(6, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
        client.sendall(header)
        print(f"[Gerenciador] ACK_DATA enviado para {addr[0]}:{addr[1]}")
        
        match list(data.keys())[0]:
            case "EMPTY":
                if data["EMPTY"]:
                    self.has_students = False
                    print("[Gerenciador] Sala esta vazia")
                    self.__start_empty_timer()
                else:
                    self.has_students = True
                    self.__cancel_empty_timer()
                    self.__send_command(1, 4)
                    self.__send_command(1, 6)
            case "NROALUNO":
                current_date = date.today().strftime("%Y-%m-%d")
                self.attendence.setdefault(current_date, []).append((data["NOME"], data["NROALUNO"]))
                print(f"[Gerenciador] Presenca registrada: {data['NOME']} ({data['NROALUNO']})")
            case "STATUS":
                if data["STATUS"]:
                    self.projector = True
                    self.__send_command(0, 4)
                    self.__send_command(1, 5)
                else:
                    self.projector = False
                    self.__send_command(1, 4)
                    self.__send_command(0, 5)
                message = "ligado" if self.projector else "desligado"
                print(f"[Gerenciador] Projetor {message}")


    def __send_command(self, command: int, actuator_type: int) -> bool:
        """Envia um comando para todos os atuadores do tipo e aguarda o ACK_COMMAND.

        Args:
            command (int): 0 para desligar, 1 para ligar.
            actuator_type (int): Tipo do atuador (4=iluminação, 5=projetor, 6=ar_condicionado).

        Returns:
            bool: True se todos os ACK_COMMAND foram recebidos, False caso contrário.
        """
        actuators = []
        for addr, conn_info in self.connections.items():
            if conn_info[1] == actuator_type:
                actuators.append((addr, conn_info[2]))

        if not actuators:
            nome = ATUADOR_NOME.get(actuator_type, f"Atuador tipo {actuator_type}")
            print(f"[Gerenciador] {nome} nao encontrado conectado")
            return False

        nome = ATUADOR_NOME.get(actuator_type, f"Atuador tipo {actuator_type}")
        comando_str = "ligar" if command else "desligar"
        print(f"\n[Gerenciador] Enviando comando para {len(actuators)}x {nome}")
        print(f"  >> Comando: {comando_str}")

        all_success = True
        for actuator_addr, actuator_socket in actuators:
            header = Header(7, (GERENCIADOR, GERENCIADOR_PORT_TCP), actuator_addr, HEADER_SIZE).to_string()
            send_data = {"command": command}
            payload = payload_format(header, json.dumps(send_data))

            delay = self.DELAY
            success = False
            for i in range(self.MAX_RETRIES):
                actuator_socket.sendall(to_bytes(payload))

                for _ in range(10):
                    time.sleep(delay / 10)
                    if self.ack_responses.pop(actuator_addr, None):
                        print(f"[Gerenciador] Comando executado por {nome} ({actuator_addr[0]}:{actuator_addr[1]})")
                        success = True
                        break
                if success:
                    break

                if i < self.MAX_RETRIES - 1:
                    delay *= 2

            if not success:
                print(f"[Gerenciador] Falha ao enviar comando para {nome} ({actuator_addr[0]}:{actuator_addr[1]})")
                all_success = False

        return all_success

    def __start_empty_timer(self):
        """Inicia o timer de 15 minutos para desligar equipamentos (req 3.3)."""
        self.__cancel_empty_timer()
        self.empty_timer = threading.Timer(5, self.__on_empty_timeout)
        self.empty_timer.daemon = True
        self.empty_timer.start()
        print("[Gerenciador] Timer de 5 segundos iniciado (sala vazia)")

    def __cancel_empty_timer(self):
        """Cancela o timer de sala vazia se estiver ativo."""
        if self.empty_timer and self.empty_timer.is_alive():
            self.empty_timer.cancel()
            self.empty_timer = None
            print("[Gerenciador] Timer de sala vazia cancelado")

    def __on_empty_timeout(self):
        """Callback do timer: desliga iluminação, projetor e ar condicionado (req 3.3)."""
        print("\n[Gerenciador] Tempo sem deteccao esgotado - desligando equipamentos")
        for atype, nome in [(4, "Sistema de Iluminacao"), (5, "Projetor"), (6, "Ar Condicionado")]:
            if self.__send_command(0, atype):
                print(f"[Gerenciador] {nome} desligado")

    def __daily_shutdown(self):
        """Thread que desliga todos os equipamentos quando o relógio marca 23h (req 3.7)."""
        print("\n[Gerenciador] Atingiu 23h - Desligando todos os equipamentos")
        self.__send_command(0, 4)
        self.__send_command(0, 5)
        self.__send_command(0, 6)

    def __send_data(self, client : socket.socket, data : dict, addr : tuple[str, str]):
        """Método que envia os dados dos alunos para o cliente.

        Esse método busca os alunos presentes no dia fornecido e envia eles.

        Args:
            client (socket.socket): Socket do cliente.
            data (dict): JSON com o payload da requisição.
            addr (tuple[str, str]): Tupla com IP e Porta
        """
        print(f"\n[Gerenciador] REQUEST_DATA recebido de {addr[0]}:{addr[1]}")

        search_date = data["DATE"]
        attendence_list = self.attendence.get(search_date, [])
        header = Header(8, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_string()
        send_data = {"DATA": attendence_list}
        payload = payload_format(header, json.dumps(send_data))

        delay = self.DELAY

        for i in range(self.MAX_RETRIES):
            client.sendall(to_bytes(payload))
            
            try:
                client.settimeout(delay)
                response = client.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
                client.settimeout(None)
                header, data = parse_payload(response)

                if header and header.tipo_primitiva == 10:
                    print(f"[Gerenciador] Dados de presenca enviados para {addr[0]}:{addr[1]}")
                    break
            except socket.timeout:
                pass

            if i < self.MAX_RETRIES - 1:
                delay *= 2

    def __client_handler(self, client : socket.socket, addr : tuple[str, str]):
        """Método para lidar com as requisições recebidas em threads.

        Esse método é executado em uma thread para cada conexão TCP. Ele
        permanece em loop processando todas as mensagens recebidas do
        dispositivo na mesma conexão.

        Args:
            client (socket.socket): Socket do client.
            addr (tuple[str, str]): Tupla que contém o IP e a Porta.
        """
        while True:
            try:
                payload = client.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
                if not payload:
                    break
                header, data = parse_payload(payload)
                match header.tipo_primitiva:
                    case 1:
                        self.__ack(client, data, addr)
                    case 2:
                        self.__ack_data(client, data, addr)
                    case 3:
                        self.ack_responses[addr] = True
                    case 9:
                        self.__send_data(client, data, addr)
                    case 12:
                        self.__daily_shutdown()
            except (ConnectionResetError, ConnectionAbortedError, OSError):
                break

        # Remove das conexões ativas
        self.connections.pop(addr, None)
        client.close()
                 
    def __tcp_socket_thread(self):
        """Thread responsável pelo funcionamento do socket TCP.

        Esse método é responsável por criar, liberar para escutar, aceitar as conexões e criar as threads para cada requisição que recebe.
        """
        # Criando socket TCP
        tcp_socket = create_socket_tcp(GERENCIADOR, GERENCIADOR_PORT_TCP)
        tcp_socket.listen()

        while True:
            client, addr = tcp_socket.accept()

            client_thread = threading.Thread(target=self.__client_handler, args=(client, addr), daemon=True)
            client_thread.start()
            
                
gerenciador = Gerenciador()
while True:
    pass
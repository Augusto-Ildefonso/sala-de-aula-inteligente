from socket_manager import create_socket_tcp, create_socket_udp
from utils import *
from global_vars import *
from datetime import date
import socket
import threading


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
0
1
2
3
4
5
6 -> ACK_DATA
7
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
        print("Socket UDP iniciado.")

        # Criando socket TCP (resto da comunicação)
        self.tcp_thread = threading.Thread(target=self.__tcp_socket_thread, daemon=True)
        self.tcp_thread.start()
        print("Socket TCP iniciado.")

        # Dispositivos conectados
        self.connections = {}

        # Lista de alunos
        self.attendence= {"2026-06-12": [("Augusto", "15441810"), ("Pedro", "11223344")]}

        # Há pessoas na sala
        self.has_students = False

        # Projetor
        self.projector = False

        # Constantes
        self.DELAY = 0.1
        self.MAX_RETRIES = 3

    def __udp_socket_thread(self):
        """Thread responsável pelo funcionamento do socket UDP.

        Ela recebe requisições de broadcast (DISCOVER), processa ela e envia o endereço TCP do gerenciador (OFFER).
        """
        udp_socket = create_socket_udp(GERENCIADOR, GERENCIADOR_PORT_UDP)
        while True:
            data, addr = udp_socket.recvfrom(1024)
            print(f"\n\nSocket UDP:\n\tDISCOVER de {addr[0]}:{addr[1]}")
            data = data.decode()
            header = Header(4, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            udp_socket.sendto(header, addr)
            print(f"\tOFFER enviado para {addr[0]}:{addr[1]}")

    def __ack(self, client : socket.socket, data : dict, addr : tuple[str, str]):
        """Método que que processa requisições do tipo SYN e devolve o ACK.

        Esse método lida com o estabelecimento da conexão e retorna a resposta ACK.

        Args:
            client (socket.socket): Socket do cliente.
            data (dict): JSON com o payload da requisição.
            addr (tuple[str, str]): Tupla com IP e Porta
        """
        print(f"\n\nSocket TCP:\n\tSYN de {addr[0]}:{addr[1]}")
        if data:
            data_type = int(data["type"])
            element_type : str
            if data_type <= 3:
                element_type = "sensor"
            else:
                element_type = "atuador"
            self.connections[addr] = (element_type, data_type)
            ack_header_bytes = Header(5, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            client.sendall(ack_header_bytes)
            print(f"\tACK enviado para {addr[0]}:{addr[1]}")

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
        print(f"\n\nSocket TCP:\n\tSEND_DATA de {addr[0]}:{addr[1]}")
        # Se não estiver conectado
        if self.__connection_check(addr):
            header = Header(11, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
            client.sendall(header)
            return
        
        match list(data.keys())[0]:
            case "EMPTY":
                if data["EMPTY"]:
                    self.has_students = False
                else:
                    self.has_students = True
                message = "Sala não está vazia" if self.has_students else "Sala está vazia"
                print(f"\t{message}")
            case "NROALUNO":
                current_date = date.today().strftime("%Y-%m-%d")
                self.attendence.setdefault(current_date, []).append((data["NOME"], data["NROALUNO"]))
                message = f"{data['NOME']} - {data['NROALUNO']}"
                print(f"\tEstudante: {message} está presente")
            case "STATUS":
                if data["STATUS"]:
                    self.projector = True # ON
                else:
                    self.projector = False # OFF
                message = "ligado" if self.projector else "desligado"
                print(f"\tO projetor está {message}")
                
        header = Header(6, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_bytes()
        client.sendall(header)
        print(f"\tACK_DATA enviado para {addr[0]}:{addr[1]}")
    
    def __send_data(self, client : socket.socket, data : dict, addr : tuple[str, str]):
        """Método que envia os dados dos alunos para o cliente.

        Esse método busca os alunos presentes no dia fornecido e envia eles.

        Args:
            client (socket.socket): Socket do cliente.
            data (dict): JSON com o payload da requisição.
            addr (tuple[str, str]): Tupla com IP e Porta
        """
        print(f"\n\nSocket TCP:\n\tREQUEST_DATA de {addr[0]}:{addr[1]}")

        # Mudança da documentação: formato recebido YYYY-mm-dd
        search_date = data["DATE"]
        attendence_list = self.attendence[search_date]
        header = Header(8, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, HEADER_SIZE).to_string()
        send_data = {"DATA": attendence_list}
        payload = payload_format(header, json.dumps(send_data))

        # Aguardando receber ACK_DATA
        delay = self.DELAY

        # Tentativas múltiplas com backoff exponencial
        for i in range(self.MAX_RETRIES):
            client.sendall(to_bytes(payload))
            print(f"\tTentativa {i}: SEND_DATA enviado para {addr[0]}:{addr[1]}")
            
            # Espera ACK do cliente
            payload = client.recv(MAX_SIZE_PAYLOAD_SEND_DATA) 
            header, data = parse_payload(payload)

            # Verifica se recebeu ACK
            if header and header.tipo_primitiva == 10:
                print(f"\tSEND_DATA enviado para {addr[0]}:{addr[1]} - sucesso")
                print(f"\tACK_DATA recebido de {addr[0]}:{addr[1]}")
                break

            # Backoff exponencial
            if i < self.MAX_RETRIES - 1:
                delay *= 2 
            
            # Imprime falha
            if i == self.MAX_RETRIES - 1:
                print(f"\tSEND_DATA enviado para {addr[0]}:{addr[1]} - falha")

    def __client_handler(self, client : socket.socket, addr : tuple[str, str]):
        """Método para lidar com as requisições recebidas em threads.

        Esse método é executado em uma thread para cada requisição. Ele lida com toda a lógica de resposta e processamento de tarefas.

        Args:
            client (socket.socket): Socket do client.
            addr (tuple[str, str]): Tupla que contém o IP e a Porta.
        """
        payload = client.recv(MAX_SIZE_PAYLOAD_SEND_DATA)
        if payload:
            header, data = parse_payload(payload)
            match header.tipo_primitiva:
                case 1:
                    self.__ack(client, data, addr)
                case 2:
                    self.__ack_data(client, data, addr)
                case 9:
                    self.__send_data(client, data, addr)
                
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
            print(f"\nThread ({client_thread.name}) criada para requisição de {(addr[0], addr[1])}")
            client_thread.start()
            
                
gerenciador = Gerenciador()
while True:
    pass
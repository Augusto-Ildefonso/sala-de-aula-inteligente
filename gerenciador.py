from socket_manager import create_socket_tcp, create_socket_udp
from utils import *
from global_vars import *
import socket
import threading

"""
Mapeamento sensores
0 -> presença
1 -> leitor
2 -> chave

Padrão do cabeçalho
TIPO_PRIMITIVA|ID_REM|ID_DEST|TAM
"""

class Gerenciador:
    def __udp_socket_thread(self):
        """Thread responsável pelo funcionamento do socket UDP

        Ela recebe requisições de broadcast (DISCOVER), processa ela e envia o endereço TCP do gerenciador (OFFER).
        """
        udp_socket = create_socket_udp(GERENCIADOR, GERENCIADOR_PORT_UDP)
        while True:
            data, addr = udp_socket.recvfrom(1024)
            print(f"\n\nSocket UDP:\n\tDISCOVER de {addr[0]}:{addr[1]}")
            data = data.decode()
            payload = payload_format(GERENCIADOR, str(GERENCIADOR_PORT_TCP))
            udp_socket.sendto(payload.encode("utf-8"), addr)
            print(f"\tOFFER enviado para {addr[0]}:{addr[1]}")

    def __ack(self, tcp_socket : socket.socket, data : dict, addr):
        # É um sensor
        if data:    
            print(f"\n\nSocket TCP:\n\tSYN de {addr[0]}:{addr[1]}")
            self.connections[addr] = ("sensor", int(data["type"]))
            ack_header_bytes = Header(5, (GERENCIADOR, GERENCIADOR_PORT_TCP), addr, 32).to_bytes()
            tcp_socket.sendall(ack_header_bytes)
            print(f"\tACK enviado para {addr[0]}:{addr[1]}")
        # É um atuador
        else:
            pass

    def __tcp_socket_thread(self):
        # Criando socket TCP
        tcp_socket = create_socket_tcp(GERENCIADOR, GERENCIADOR_PORT_TCP)
        tcp_socket.listen()

        while True:
            client, addr = tcp_socket.accept()
            payload = client.recv(MAX_SIZE_PAYLOAD_SEND_DATA)

            if payload:
                header, data = parse_payload(payload)
                match header.tipo_primitiva:
                    case 1:
                        self.__ack(client, data, addr)
                    case 2:
                        pass
                    case 3:
                        pass
                    case 4:
                        pass
                    case 5:
                        pass
                    case 6:
                        pass
                    case 7:
                        pass
                    case 8:
                        pass
                    case 9:
                        pass
                

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

gerenciador = Gerenciador()
while True:
    pass
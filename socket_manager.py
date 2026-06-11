import socket

def create_socket_tcp(GERENCIADOR : str, port: int) -> socket.socket:
    # AF_INET -> IPv4, SOCK_STREAM -> TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((GERENCIADOR, port))
    
    return s

def create_socket_udp(GERENCIADOR : str, port: int) -> socket.socket:
    # AF_INET -> IPv4, SOCK_DGRAM -> UDP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((GERENCIADOR, port))
    
    return s
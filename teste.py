import socket
import time

def executar_sensor():
    # ==========================================
    # PASSO 1: Descoberta via UDP (Broadcast)
    # ==========================================
    sensor_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sensor_udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sensor_udp.settimeout(3.0)

    # Em uma rede real, o destino seria ('255.255.255.255', 3001)
    destino_broadcast = ('127.0.0.1', 3001) 
    
    ip_gerenciador = None
    porta_tcp_gerenciador = 3000 # Assumindo a porta padrão do seu código

    print("[UDP] Procurando Gerenciador na rede...")
    try:
        sensor_udp.sendto(b"DISCOVER", destino_broadcast)
        
        # Recebe a resposta e o endereço de quem respondeu
        dados_payload, endereco_origem = sensor_udp.recvfrom(1024)
        
        # O IP real de quem respondeu está no índice 0 da tupla de endereço
        ip_gerenciador = endereco_origem[0]
        
        print(f"[UDP] SUCESSO! Gerenciador encontrado no IP: {ip_gerenciador}")
        print(f"[UDP] Payload recebido: {dados_payload.decode('utf-8')}")
        
        # DICA: Se o seu 'payload_format' retornar a porta TCP dinamicamente dentro 
        # da string de dados, você faria o "parse" dessa string aqui para
        # substituir a variável 'porta_tcp_gerenciador'.

    except socket.timeout:
        print("[UDP] ERRO: Timeout. Nenhum Gerenciador respondeu ao Discover.")
        sensor_udp.close()
        return # Encerra o script se não achar o gerenciador
    
    # Fecha o socket UDP, pois o trabalho de descoberta terminou
    sensor_udp.close()
    
    # Dá uma pequena pausa (opcional, apenas para o log ficar legível)
    time.sleep(1)

    # ==========================================
    # PASSO 2: Conexão e Handshake via TCP
    # ==========================================
    print(f"\n[TCP] Iniciando conexão com {ip_gerenciador}:{porta_tcp_gerenciador}...")
    
    sensor_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sensor_tcp.settimeout(3.0)

    try:
        # Conecta no IP que acabamos de descobrir no Passo 1
        sensor_tcp.connect((ip_gerenciador, porta_tcp_gerenciador))
        print("[TCP] Túnel estabelecido!")

        # Envia a mensagem SYN da sua aplicação
        mensagem_syn = '1|123.123.123.123:3000|123.123.123.123:3001|1|{"type": 1}'
        print(f"[TCP] Enviando Handshake: '{mensagem_syn}'")
        sensor_tcp.sendall(mensagem_syn.encode('utf-8'))

        # Aguarda o ACK do Gerenciador
        resposta = sensor_tcp.recv(1024)
        if resposta:
            print(f"[TCP] Resposta do Gerenciador recebida: {resposta.decode('utf-8')}")
            print("\n--- SENSOR AUTENTICADO E PRONTO PARA ENVIAR DADOS! ---")
        else:
            print("[TCP] A conexão foi fechada pelo Gerenciador sem resposta.")

    except ConnectionRefusedError:
        print("[TCP] ERRO: O IP foi encontrado, mas a porta TCP está recusando conexões.")
    except socket.timeout:
        print("[TCP] AVISO: O Gerenciador recebeu o SYN, mas não respondeu (Timeout).")
    finally:
        # Encerra o socket de teste
        sensor_tcp.close()
        print("\nScript do sensor finalizado.")

if __name__ == "__main__":
    executar_sensor()
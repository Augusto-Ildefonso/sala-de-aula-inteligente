import socket
import json
import time

# Constantes do seu projeto
TIPO_DISCOVER = 0
TIPO_SYN = 1
TIPO_ACK_COMMAND = 3
TIPO_SEND_COMMAND = 7

# Função auxiliar para gerar o pacote (Header 47 bytes padronizado + JSON)
def criar_pacote(tipo: int, ip_rem: str, porta_rem: int, ip_dest: str, porta_dest: int, dados: dict) -> bytes:
    rem_str = f"{ip_rem}:{porta_rem}"
    dest_str = f"{ip_dest}:{porta_dest}"
    
    payload_str = json.dumps(dados) if dados else ""
    tam_total = 47 + len(payload_str) # 47 fixos do header + tamanho do payload
    
    # Monta o header forçando os tamanhos corretos com padding e zfill
    header = f"{str(tipo)[:1]}{rem_str:<21}{dest_str:<21}{str(tam_total).zfill(4)}"
    return (header + payload_str).encode('utf-8')

def executar_atuador():
    # ==========================================
    # PASSO 1: Descoberta via UDP (DISCOVER - 0)
    # ==========================================
    atuador_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    atuador_udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    atuador_udp.settimeout(3.0)

    # Identificadores locais (para o teste)
    meu_ip = '127.0.0.1'
    porta_udp_origem = atuador_udp.getsockname()[1]
    
    print("[UDP] Buscando Gerenciador...")
    pacote_discover = criar_pacote(TIPO_DISCOVER, meu_ip, porta_udp_origem, '255.255.255.255', 3001, None)
    
    try:
        atuador_udp.sendto(pacote_discover, ('127.0.0.1', 3001))
        dados_offer, end_gerenciador = atuador_udp.recvfrom(1024)
        
        # O gerenciador retornou o IP e Porta dele. Assumindo a porta 3000 padrão:
        ip_ger = end_gerenciador[0]
        porta_tcp_ger = 3000
        print(f"[UDP] OFFER recebido de {ip_ger}:{end_gerenciador[1]}")
        
    except socket.timeout:
        print("[Erro] Nenhum Gerenciador respondeu. Abortando.")
        return
    finally:
        atuador_udp.close()

    time.sleep(1)

    # ==========================================
    # PASSO 2: Conexão e Handshake TCP (SYN - 1)
    # ==========================================
    atuador_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    atuador_tcp.connect((ip_ger, porta_tcp_ger))
    
    # Pegamos a porta real que o SO nos deu para a conexão TCP
    porta_tcp_origem = atuador_tcp.getsockname()[1] 
    
    print(f"\n[TCP] Conectado ao Gerenciador! Enviando SYN (1)...")
    pacote_syn = criar_pacote(TIPO_SYN, meu_ip, porta_tcp_origem, ip_ger, porta_tcp_ger, None)
    atuador_tcp.sendall(pacote_syn)

    # Aguarda o ACK (5) do Gerenciador
    dados_ack = atuador_tcp.recv(1024)
    if dados_ack:
        tipo_recebido = int(dados_ack[0:1].decode())
        print(f"[TCP] Recebido tipo {tipo_recebido} (Esperado ACK - 5). Atuador registrado!")

    # ==========================================
    # PASSO 3: Loop escutando SEND_COMMAND (7)
    # ==========================================
    print("\n[ATUADOR] Em espera por comandos do Gerenciador (SEND_COMMAND)...")
    try:
        while True:
            comando_raw = atuador_tcp.recv(1024)
            if not comando_raw:
                print("[TCP] O Gerenciador encerrou a conexão.")
                break
            
            tipo_msg = int(comando_raw[0:1].decode())
            
            # Se for um SEND_COMMAND (7) (Requisito 3.2 e seguintes)
            if tipo_msg == TIPO_SEND_COMMAND:
                payload_json = comando_raw[47:].decode('utf-8')
                dados_comando = json.loads(payload_json)
                
                estado = "LIGAR" if dados_comando.get("command") == 1 else "DESLIGAR"
                print(f"> Comando recebido: {estado} atuador! Executando ação física...")
                
                # Simula um tempinho para a ação física acontecer
                time.sleep(1)
                
                # Responde com ACK_COMMAND (3) (Requisito 2.2.3)
                print("< Ação concluída. Enviando ACK_COMMAND (3) com status 1 (Sucesso).")
                payload_resposta = {"status": 1}
                pacote_ack_cmd = criar_pacote(TIPO_ACK_COMMAND, meu_ip, porta_tcp_origem, ip_ger, porta_tcp_ger, payload_resposta)
                atuador_tcp.sendall(pacote_ack_cmd)

    except KeyboardInterrupt:
        print("\nDesligando atuador de teste...")
    finally:
        atuador_tcp.close()

if __name__ == "__main__":
    executar_atuador()
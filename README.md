# Sala de Aula Inteligente — SSC0142 Redes de Computadores

Projeto de simulação de uma sala de aula inteligente com sensores, atuadores e um gerenciador central, comunicando-se via sockets UDP/TCP com protocolo próprio.

## Estrutura do Projeto

```
.
├── global_vars.py      # Constantes de configuração (IP, portas, tamanhos)
├── header.py           # Classe Header do protocolo customizado
├── utils.py            # Funções utilitárias (formatação/parse de payload, JSON)
├── socket_manager.py   # Factory de sockets TCP e UDP
├── gerenciador.py      # Gerenciador central — orquestra sensores e atuadores
├── sensor.py           # Sensor (presença, leitor de cartão, chave do projetor)
├── atuador.py          # Atuador (iluminação, projetor, ar-condicionado)
├── cliente.py          # Cliente (professor) — consulta lista de presença
├── sala_de_aula.py     # Orquestrador da simulação (modos aleatório e controlado)
├── Makefile            # Atalhos para execução
├── arquitetura.md      # Documentação completa da arquitetura do protocolo
├── alteracoes.md       # Divergências entre arquitetura original e implementação
└── trabalho_pratico_redes.pdf  # Enunciado do trabalho
```

## Visão Geral dos Componentes

### `global_vars.py`
Define constantes compartilhadas: endereço do gerenciador (`127.0.0.1`), portas TCP (3000) e UDP (3001), tamanho máximo de payload (1024) e tamanho do header (32).

### `header.py`
Implementa a classe `Header` do protocolo customizado. O header é uma string no formato `TIPO_PRIMITIVA|ID_REM|ID_DEST|TAM`, com métodos `to_bytes()` e `to_string()` para serialização.

### `utils.py`
Funções de apoio:
- `payload_format()` — junta header + dados com separador `|`
- `parse_payload()` — faz o parsing reverso, retornando tupla `(Header, dict)`
- `to_bytes()` / `convert_from_json()` / `convert_to_json()` — conversão entre bytes, string e JSON

### `socket_manager.py`
Duas funções simples que criam e retornam sockets TCP (`SOCK_STREAM`) e UDP (`SOCK_DGRAM`) já vinculados ao endereço do gerenciador.

### `gerenciador.py`
O cérebro do sistema. Executa duas threads principais:
- **Thread UDP**: escuta broadcast `DISCOVER` e responde com `OFFER` (endereço TCP)
- **Thread TCP**: aceita conexões e cria uma thread por cliente (sensor/atuador/cliente)

Lógica de controle implementada:
- **Req 3.2**: Sensor de presença detecta pessoas → liga iluminação (4) e ar-condicionado (6)
- **Req 3.3**: Sala vazia por 15 min → desliga tudo (timer interno)
- **Req 3.4**: Chave do projetor ligada → apaga luzes, liga projetor
- **Req 3.5**: Chave do projetor desligada → acende luzes, desliga projetor
- **Req 3.6**: Leitor de cartão → registra presença do aluno
- **Req 3.7**: 23:00 → desliga todos os equipamentos automaticamente
- **Req 3.8/4**: Cliente requisita lista de presença → gerenciador envia os dados

Mapeamento de tipos:
- Sensores: 0 = presença, 1 = leitor, 2 = chave
- Atuadores: 4 = iluminação, 5 = projetor, 6 = ar-condicionado

Primitivas do protocolo:
| Código | Nome            | Direção                |
|--------|-----------------|------------------------|
| 0      | DISCOVER        | Sensor/Atuador → Gerenciador (UDP) |
| 1      | SYN             | Dispositivo → Gerenciador (TCP)    |
| 2      | SEND_DATA       | Sensor → Gerenciador               |
| 3      | ACK_COMMAND     | Atuador → Gerenciador              |
| 4      | OFFER           | Gerenciador → Dispositivo (UDP)    |
| 5      | ACK             | Gerenciador → Dispositivo          |
| 6      | ACK_DATA        | Gerenciador → Sensor               |
| 7      | SEND_COMMAND    | Gerenciador → Atuador              |
| 8      | SEND_DATA       | Gerenciador → Cliente              |
| 9      | REQUEST_DATA    | Cliente → Gerenciador              |
| 10     | ACK_DATA        | Cliente → Gerenciador              |
| 11     | NÃO CONECTADO   | Gerenciador → Dispositivo          |

### `sensor.py`
Classe `Sensor` que se conecta ao gerenciador via:
1. **DISCOVER** (UDP broadcast) → recebe **OFFER** com o endereço TCP
2. **SYN** (TCP) → recebe **ACK** e estabelece conexão
3. **SEND_DATA** (TCP) → envia dados e aguarda **ACK_DATA**

Método público `send_data(data)` com retry e backoff exponencial.

### `atuador.py`
Classe `Atuador` que se conecta ao gerenciador (mesmo fluxo DISCOVER → SYN) e entra em um loop de escuta (`__listen()`) aguardando comandos **SEND_COMMAND**. Ao receber um comando, executa a ação (liga/desliga) e responde com **ACK_COMMAND**.

### `cliente.py`
Classe `Cliente` que se conecta ao gerenciador (DISCOVER → SYN) e oferece o método `request_data(date)` para consultar a lista de presença de uma data específica, com retry e backoff exponencial.

### `sala_de_aula.py`
Orquestrador da simulação. Usa `multiprocessing` e `subprocess` para rodar todos os componentes em processos separados.

Fluxo de execução:
1. **Configuração interativa**: usuário define quantos sensores/atuadores e seus tipos, e se inclui o cliente
2. **Inicialização**: gerencia os processos filhos (gerenciador, sensores, atuadores, cliente)
3. **Simulação**: dois modos:
   - **Aleatório**: gera eventos automaticamente por 30 passos, testando todos os requisitos
   - **Controlado**: menu interativo onde o usuário escolhe cada ação
4. **Cleanup**: finaliza todos os processos ao encerrar

## Como Executar

### Pré-requisitos
- Python 3.8+
- Sistema Unix-like (macOS/Linux) — necessário para `subprocess`

### Executar a simulação completa

```bash
python3 sala_de_aula.py
```

O programa guiará você pela configuração interativa e pela escolha do modo de simulação.

### Executar apenas o gerenciador (para testes manuais)

```bash
make gerenciador
# ou
python3 gerenciador.py
```

### Testes

O projeto não possui suíte de testes automatizada. A validação é feita via simulação (`sala_de_aula.py`) nos modos aleatório e controlado, que exercitam todos os requisitos funcionais.

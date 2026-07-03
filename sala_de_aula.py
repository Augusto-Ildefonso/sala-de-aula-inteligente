import subprocess
import multiprocessing
import sys
import os
import time
import random

from sensor import Sensor
from atuador import Atuador
from cliente import Cliente


SENSOR_TYPES = {
    "presenca": 0,
    "leitor": 1,
    "chave": 2,
}

ACTUATOR_TYPES = {
    "iluminacao": 4,
    "projetor": 5,
    "ar_condicionado": 6,
}

SENSOR_TYPE_NAMES = {v: k for k, v in SENSOR_TYPES.items()}
ACTUATOR_TYPE_NAMES = {v: k for k, v in ACTUATOR_TYPES.items()}

STUDENTS = [
    ("Augusto", "15441810"),
    ("Pedro", "11223344"),
    ("Ana", "99887766"),
    ("Maria", "55443322"),
    ("Joao", "33221100"),
]


def _sensor_process(sensor_id: str, sensor_type: int, cmd_queue: multiprocessing.Queue):
    """Processo filho que executa um sensor.

    Cria a instancia do Sensor, aguarda comandos pela fila e executa
    o envio de dados quando solicitado.

    Args:
        sensor_id: Identificador unico do sensor (ex: "sensor_1").
        sensor_type: Tipo do sensor (0=presenca, 1=leitor, 2=chave).
        cmd_queue: Fila multiprocessing para receber comandos.
    """
    sensor = Sensor(sensor_type)
    time.sleep(0.5)
    print(f"[Sala] Sensor {sensor_id} ({SENSOR_TYPE_NAMES[sensor_type]}) pronto.")
    while True:
        try:
            cmd = cmd_queue.get()
            if cmd.get("action") == "send_data":
                sensor.send_data(cmd["data"])
            elif cmd.get("action") == "stop":
                break
        except Exception as e:
            print(f"[Erro-{sensor_id}]: {e}")


def _actuator_process(actuator_id: str, actuator_type: int):
    """Processo filho que executa um atuador.

    Cria a instancia do Atuador e mantem o processo vivo para que
    ele continue escutando comandos do gerenciador.

    Args:
        actuator_id: Identificador unico do atuador (ex: "atuador_1").
        actuator_type: Tipo do atuador (4=iluminacao, 5=projetor, 6=ar_condicionado).
    """
    Atuador(actuator_type)
    print(f"[Sala] Atuador {actuator_id} ({ACTUATOR_TYPE_NAMES[actuator_type]}) pronto.")
    while True:
        time.sleep(1)


def _client_process(cmd_queue: multiprocessing.Queue):
    """Processo filho que executa o cliente.

    Cria a instancia do Cliente, aguarda comandos pela fila e executa
    a requisicao de dados quando solicitado.

    Args:
        cmd_queue: Fila multiprocessing para receber comandos.
    """
    client = Cliente()
    time.sleep(0.5)
    print("[Sala] Cliente pronto.")
    while True:
        try:
            cmd = cmd_queue.get()
            if cmd.get("action") == "request_data":
                client.request_data(cmd["date"])
            elif cmd.get("action") == "stop":
                break
        except Exception as e:
            print(f"[Erro-Cliente]: {e}")


class SalaDeAula:
    """Coordena a simulacao da sala de aula.

    Cria e gerencia processos separados para o gerenciador, sensores,
    atuadores e cliente, permitindo simular os fluxos definidos nos
    requisitos funcionais.
    """

    def __init__(self):
        self.sensor_configs: list[tuple[str, int, str]] = []
        self.actuator_configs: list[tuple[str, int, str]] = []
        self.has_client = False

        self.gerenciador_proc: subprocess.Popen | None = None
        self.sensor_procs: dict[str, dict] = {}
        self.actuator_procs: dict[str, dict] = {}
        self.client_proc: multiprocessing.Process | None = None
        self.client_queue: multiprocessing.Queue | None = None

    @staticmethod
    def _input_int(prompt: str, min_val: int = 0, max_val: int = 100) -> int:
        while True:
            try:
                val = int(input(prompt))
                if min_val <= val <= max_val:
                    return val
                print(f"Valor deve estar entre {min_val} e {max_val}.")
            except ValueError:
                print("Entrada invalida. Digite um numero.")

    @staticmethod
    def _input_option(prompt: str, options: dict) -> tuple:
        items = list(options.items())
        print(prompt)
        for i, (name, _) in enumerate(items, 1):
            print(f"  {i}. {name}")
        while True:
            try:
                idx = int(input("Escolha: ")) - 1
                if 0 <= idx < len(items):
                    return items[idx][1], items[idx][0]
                print(f"Escolha entre 1 e {len(items)}.")
            except ValueError:
                print("Entrada invalida.")

    # ----------------------------------------------------------------
    # Configuracao
    # ----------------------------------------------------------------

    def setup_interactive(self):
        """Questionario interativo para configurar os componentes."""
        print("\n============================================================")
        print("         CONFIGURACAO DA SALA DE AULA")
        print("============================================================")

        n = self._input_int("\nQuantos sensores? ", 0, 20)
        for i in range(1, n + 1):
            print(f"\n  --- Sensor {i} ---")
            tipo, nome = self._input_option("  Tipo do sensor:", SENSOR_TYPES)
            sid = f"sensor_{i}"
            self.sensor_configs.append((sid, tipo, nome))
            print(f"  Sensor {sid} configurado como {nome}.")

        n = self._input_int("\nQuantos atuadores? ", 0, 20)
        for i in range(1, n + 1):
            print(f"\n  --- Atuador {i} ---")
            tipo, nome = self._input_option("  Tipo do atuador:", ACTUATOR_TYPES)
            aid = f"atuador_{i}"
            self.actuator_configs.append((aid, tipo, nome))
            print(f"  Atuador {aid} configurado como {nome}.")

        r = input("\nIncluir cliente (professor)? (s/n): ").strip().lower()
        self.has_client = r in ("s", "sim", "y", "yes")

        print("\n------------------------------------------------------------")
        print("Resumo da configuracao:")
        for sid, st, sn in self.sensor_configs:
            print(f"  Sensor: {sid} ({sn})")
        for aid, at, an in self.actuator_configs:
            print(f"  Atuador: {aid} ({an})")
        print(f"  Cliente: {'sim' if self.has_client else 'nao'}")
        print("------------------------------------------------------------")

    # ----------------------------------------------------------------
    # Inicializacao dos componentes
    # ----------------------------------------------------------------

    def start_all(self):
        """Inicia todos os componentes em processos separados."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        print("\n============================================================")
        print("         INICIANDO COMPONENTES")
        print("============================================================")

        gerenciador_path = os.path.join(base_dir, "gerenciador.py")
        print("\nIniciando Gerenciador...")
        self.gerenciador_proc = subprocess.Popen(
            [sys.executable, gerenciador_path],
            cwd=base_dir,
        )
        time.sleep(1)

        for sid, st, sn in self.sensor_configs:
            print(f"Iniciando {sid} ({sn})...")
            q = multiprocessing.Queue()
            p = multiprocessing.Process(
                target=_sensor_process, args=(sid, st, q), daemon=True
            )
            p.start()
            self.sensor_procs[sid] = {
                "process": p, "queue": q, "type": st, "name": sn,
            }

        for aid, at, an in self.actuator_configs:
            print(f"Iniciando {aid} ({an})...")
            p = multiprocessing.Process(
                target=_actuator_process, args=(aid, at), daemon=True
            )
            p.start()
            self.actuator_procs[aid] = {
                "process": p, "type": at, "name": an,
            }

        if self.has_client:
            print("Iniciando Cliente...")
            q = multiprocessing.Queue()
            p = multiprocessing.Process(
                target=_client_process, args=(q,), daemon=True
            )
            p.start()
            self.client_proc = p
            self.client_queue = q

        print("\nAguardando conexao dos componentes...")
        time.sleep(3)
        print("\n============================================================")
        print("         TODOS OS COMPONENTES INICIADOS")
        print("============================================================")

    def cleanup(self):
        """Finaliza todos os processos."""
        print("\n============================================================")
        print("         FINALIZANDO COMPONENTES")
        print("============================================================")

        for sid in self.sensor_procs:
            try:
                self.sensor_procs[sid]["queue"].put({"action": "stop"})
                self.sensor_procs[sid]["process"].terminate()
            except Exception:
                pass

        for aid in self.actuator_procs:
            try:
                self.actuator_procs[aid]["process"].terminate()
            except Exception:
                pass

        if self.client_proc:
            try:
                self.client_queue.put({"action": "stop"})
                self.client_proc.terminate()
            except Exception:
                pass

        if self.gerenciador_proc:
            self.gerenciador_proc.terminate()
            self.gerenciador_proc.wait()

        print("Todos os componentes finalizados.")

    # ----------------------------------------------------------------
    # Utilitarios para acoes
    # ----------------------------------------------------------------

    def _send_sensor(self, sid: str, data: dict):
        """Envia comando de dados para um sensor."""
        self.sensor_procs[sid]["queue"].put({
            "action": "send_data", "data": data,
        })

    def _executar_presenca_detectada(self, sid: str):
        """Sensor de presenca: detecta pessoas na sala (Req 3.2)."""
        tipo = SENSOR_TYPE_NAMES.get(self.sensor_procs[sid]["type"], "?")
        print(f"\n[Acao] Sensor {sid} ({tipo}): detectar pessoas na sala")
        self._send_sensor(sid, {"EMPTY": 0})
        print("[Resultado Esperado] Gerenciador liga iluminacao e ar condicionado (Req 3.2)")

    def _executar_presenca_vazia(self, sid: str):
        """Sensor de presenca: sala vazia (Req 3.3 - inicio do timer)."""
        tipo = SENSOR_TYPE_NAMES.get(self.sensor_procs[sid]["type"], "?")
        print(f"\n[Acao] Sensor {sid} ({tipo}): sala vazia")
        self._send_sensor(sid, {"EMPTY": 1})
        print("[Resultado Esperado] Gerenciador inicia timer de 15 minutos (Req 3.3)")

    def _executar_leitor(self, sid: str, nome: str, nro: str):
        """Leitor de cartao: registra presenca de aluno (Req 3.6)."""
        tipo = SENSOR_TYPE_NAMES.get(self.sensor_procs[sid]["type"], "?")
        print(f"\n[Acao] Sensor {sid} ({tipo}): registrar presenca - {nome} ({nro})")
        self._send_sensor(sid, {"NROALUNO": nro, "NOME": nome})
        print("[Resultado Esperado] Gerenciador salva aluno na lista de presenca (Req 3.6)")

    def _executar_chave(self, sid: str, estado: int):
        """Chave do projetor: liga/desliga (Req 3.4 e 3.5)."""
        tipo = SENSOR_TYPE_NAMES.get(self.sensor_procs[sid]["type"], "?")
        label = "ligar" if estado else "desligar"
        print(f"\n[Acao] Sensor {sid} ({tipo}): {label} chave do projetor")
        self._send_sensor(sid, {"STATUS": estado})
        if estado:
            print("[Resultado Esperado] Gerenciador apaga luzes e liga projetor (Req 3.4)")
        else:
            print("[Resultado Esperado] Gerenciador liga luzes e desliga projetor (Req 3.5)")

    def _executar_cliente(self, data: str):
        """Cliente requisita lista de presenca (Req 3.8 / 4)."""
        print(f"\n[Acao] Cliente: requisitar lista de presenca para {data}")
        self.client_queue.put({"action": "request_data", "date": data})
        print("[Resultado Esperado] Gerenciador retorna lista de alunos presentes (Req 3.8/4)")

    def _executar_timeout(self):
        """Simula o timeout de 15 minutos apos sala vazia (Req 3.3)."""
        print("\n[Acao] Simular timeout de 15 minutos (sala vazia)")
        print("[Resultado Esperado] Gerenciador desliga iluminacao, projetor e ar condicionado (Req 3.3)")

    def _executar_shutdown_23h(self):
        """Simula o desligamento geral das 23h (Req 3.7)."""
        print("\n[Acao] Simular desligamento das 23h")
        print("[Resultado Esperado] Gerenciador desliga todos os equipamentos (Req 3.7)")

    # ----------------------------------------------------------------
    # Modo aleatorio
    # ----------------------------------------------------------------

    def random_simulation(self):
        """Executa a simulacao aleatoria gerando eventos que testam todos os requisitos."""
        print("\n============================================================")
        print("         SIMULACAO ALEATORIA")
        print("============================================================")
        print("Pressione Ctrl+C para encerrar.\n")

        presenca = [sid for sid, info in self.sensor_procs.items() if info["type"] == 0]
        leitores = [sid for sid, info in self.sensor_procs.items() if info["type"] == 1]
        chaves   = [sid for sid, info in self.sensor_procs.items() if info["type"] == 2]

        room_empty = True
        student_idx = 0
        steps = 0

        while steps < 30:
            time.sleep(2)
            steps += 1
            print(f"\n{'='*60}")
            print(f"  Passo {steps}/30")
            print(f"{'='*60}")

            available = []
            if presenca:
                available.append("presenca")
            if leitores:
                available.append("leitor")
            if chaves:
                available.append("chave")
            if self.has_client:
                available.append("cliente")
            available.append("timeout")
            available.append("shutdown_23h")

            action = random.choice(available)

            if action == "presenca":
                sid = random.choice(presenca)
                if room_empty:
                    self._executar_presenca_detectada(sid)
                    room_empty = False
                else:
                    self._executar_presenca_vazia(sid)
                    room_empty = True

            elif action == "leitor":
                sid = random.choice(leitores)
                nome, nro = STUDENTS[student_idx % len(STUDENTS)]
                student_idx += 1
                self._executar_leitor(sid, nome, nro)

            elif action == "chave":
                sid = random.choice(chaves)
                estado = random.randint(0, 1)
                self._executar_chave(sid, estado)

            elif action == "cliente":
                data = time.strftime("%Y-%m-%d")
                self._executar_cliente(data)

            elif action == "timeout":
                self._executar_timeout()

            elif action == "shutdown_23h":
                self._executar_shutdown_23h()

        print("\n============================================================")
        print("         SIMULACAO ALEATORIA CONCLUIDA")
        print("============================================================")

    # ----------------------------------------------------------------
    # Modo controlado
    # ----------------------------------------------------------------

    def controlled_simulation(self):
        """Executa a simulacao controlada com menu interativo."""
        print("\n============================================================")
        print("         SIMULACAO CONTROLADA")
        print("============================================================")

        leitor_idx = {}  # controla qual aluno sera usado em cada leitor

        while True:
            print("\n" + "="*60)
            print("MENU DE CONTROLE")
            print("="*60)

            menu: list[tuple] = []
            num = 1

            for sid, info in self.sensor_procs.items():
                t = info["type"]
                nome = info["name"]

                if t == 0:
                    print(f"{num}. Sensor {sid} ({nome}): Detectar pessoas na sala")
                    menu.append(("presenca_detecta", sid))
                    num += 1
                    print(f"{num}. Sensor {sid} ({nome}): Sala vazia")
                    menu.append(("presenca_vazia", sid))
                    num += 1

                elif t == 1:
                    print(f"{num}. Sensor {sid} ({nome}): Registrar presenca de aluno")
                    menu.append(("leitor", sid))
                    num += 1

                elif t == 2:
                    print(f"{num}. Sensor {sid} ({nome}): Ligar chave do projetor")
                    menu.append(("chave", sid, 1))
                    num += 1
                    print(f"{num}. Sensor {sid} ({nome}): Desligar chave do projetor")
                    menu.append(("chave", sid, 0))
                    num += 1

            if self.has_client:
                print(f"{num}. Cliente: Requisitar lista de presenca")
                menu.append(("cliente",))
                num += 1

            print(f"{num}. Simular timeout de 15 minutos (sala vazia)")
            menu.append(("simular_timeout",))
            num += 1

            print(f"{num}. Simular desligamento das 23h")
            menu.append(("shutdown_23h",))
            num += 1

            print(f"{num}. Sair")
            menu.append(("sair",))
            num += 1

            escolha = self._input_int(f"\nEscolha uma acao (1-{len(menu)}): ", 1, len(menu))
            acao = menu[escolha - 1]

            print("\n" + "-"*50)

            if acao[0] == "presenca_detecta":
                self._executar_presenca_detectada(acao[1])

            elif acao[0] == "presenca_vazia":
                self._executar_presenca_vazia(acao[1])

            elif acao[0] == "leitor":
                sid = acao[1]
                if sid not in leitor_idx:
                    leitor_idx[sid] = 0
                idx = leitor_idx[sid]
                nome, nro = STUDENTS[idx % len(STUDENTS)]
                leitor_idx[sid] += 1

                nome_input = input(f"Nome do aluno (Enter para '{nome}'): ").strip()
                nro_input = input(f"Numero do aluno (Enter para '{nro}'): ").strip()
                nome_final = nome_input if nome_input else nome
                nro_final = nro_input if nro_input else nro
                self._executar_leitor(sid, nome_final, nro_final)

            elif acao[0] == "chave":
                self._executar_chave(acao[1], acao[2])

            elif acao[0] == "cliente":
                data = input("Data (YYYY-MM-DD) [Enter para hoje]: ").strip()
                if not data:
                    data = time.strftime("%Y-%m-%d")
                self._executar_cliente(data)

            elif acao[0] == "simular_timeout":
                self._executar_timeout()

            elif acao[0] == "shutdown_23h":
                self._executar_shutdown_23h()

            elif acao[0] == "sair":
                print("Encerrando simulacao controlada.")
                break

            input("\nPressione Enter para continuar...")

    # ----------------------------------------------------------------
    # Ponto de entrada
    # ----------------------------------------------------------------

    def run(self):
        """Ponto de entrada: configura, inicializa e executa a simulacao."""
        try:
            self.setup_interactive()
            self.start_all()

            modo = input(
                "\nModo de simulacao:\n"
                "  1. Aleatoria\n"
                "  2. Controlada\n"
                "Escolha: "
            ).strip()

            if modo == "1":
                self.random_simulation()
            else:
                self.controlled_simulation()

        except KeyboardInterrupt:
            print("\n\nSimulacao interrompida pelo usuario.")
        finally:
            self.cleanup()


if __name__ == "__main__":
    sala = SalaDeAula()
    sala.run()

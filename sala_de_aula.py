import socket
import subprocess
import multiprocessing
import sys
import os
import time


from sensor import Sensor
from atuador import Atuador
from cliente import Cliente
from header import Header
from global_vars import GERENCIADOR, GERENCIADOR_PORT_TCP, HEADER_SIZE


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


def _actuator_process(actuator_id: str, actuator_type: int, state_shared):
    """Processo filho que executa um atuador.

    Cria a instancia do Atuador e mantem o processo vivo para que
    ele continue escutando comandos do gerenciador.

    Args:
        actuator_id: Identificador unico do atuador (ex: "atuador_1").
        actuator_type: Tipo do atuador (4=iluminacao, 5=projetor, 6=ar_condicionado).
        state_shared: multiprocessing.Value compartilhado para estado.
    """
    Atuador(actuator_type, state_shared)
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
            state_shared = multiprocessing.Value('i', 0)
            p = multiprocessing.Process(
                target=_actuator_process, args=(aid, at, state_shared), daemon=True
            )
            p.start()
            self.actuator_procs[aid] = {
                "process": p, "type": at, "name": an,
                "state_shared": state_shared,
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

    def _trigger_sensor(self, sid: str, data: dict):
        """Envia comando de dados para um sensor."""
        self.sensor_procs[sid]["queue"].put({
            "action": "send_data", "data": data,
        })

    def _find_sensors(self, sensor_type: int) -> list[str]:
        return [sid for sid, info in self.sensor_procs.items() if info["type"] == sensor_type]

    def _find_actuators(self, actuator_type: int) -> list[str]:
        return [aid for aid, info in self.actuator_procs.items() if info["type"] == actuator_type]

    def _select_sensors(self, sensor_type: int) -> list[str]:
        sensors = self._find_sensors(sensor_type)
        if not sensors:
            return []
        if len(sensors) == 1:
            return sensors

        nome = SENSOR_TYPE_NAMES.get(sensor_type, f"tipo {sensor_type}")
        print(f"\nMultiplos sensores de {nome} disponiveis:")
        for i, sid in enumerate(sensors, 1):
            print(f"  {i}. {sid}")
        print(f"  {len(sensors)+1}. Todos")
        while True:
            try:
                choice = int(input("Escolha qual sensor enviar o dado: "))
                if 1 <= choice <= len(sensors):
                    return [sensors[choice-1]]
                elif choice == len(sensors)+1:
                    return sensors
                print(f"Escolha entre 1 e {len(sensors)+1}.")
            except ValueError:
                print("Entrada invalida.")

    def _tem_sensor(self, sensor_type: int) -> bool:
        return len(self._find_sensors(sensor_type)) > 0

    def _tem_atuador(self, actuator_type: int) -> bool:
        return len(self._find_actuators(actuator_type)) > 0

    def _get_actuator_states(self) -> dict[int, int]:
        states = {}
        for info in self.actuator_procs.values():
            atype = info["type"]
            shared = info["state_shared"]
            states[atype] = shared.value
        return states

    def _estado_str(self, state: int) -> str:
        return "ligado" if state else "desligado"

    def _req_3_2(self):
        print("\n" + "="*60)
        print("REQUISITO 3.2 - Presenca detectada na sala")
        print("="*60)
        print()

        sensors = self._select_sensors(0)
        if not sensors:
            print("[ERRO] Nenhum sensor de presenca configurado.")
            return
        if not self._tem_atuador(4):
            print("[ERRO] Nenhum sistema de iluminacao configurado.")
            return
        if not self._tem_atuador(6):
            print("[ERRO] Nenhum ar condicionado configurado.")
            return

        for sid in sensors:
            print(f"[Sensor de Presenca] Detectou pessoas na sala")
            self._trigger_sensor(sid, {"EMPTY": 0})
            time.sleep(0.5)
        time.sleep(2)

    def _req_3_3(self):
        print("\n" + "="*60)
        print("REQUISITO 3.3 - Sala vazia (timer de 15 minutos)")
        print("="*60)
        print()

        sensors = self._select_sensors(0)
        if not sensors:
            print("[ERRO] Nenhum sensor de presenca configurado.")
            return
        if not self._tem_atuador(4):
            print("[ERRO] Nenhum sistema de iluminacao configurado.")
            return
        if not self._tem_atuador(5):
            print("[ERRO] Nenhum projetor configurado.")
            return
        if not self._tem_atuador(6):
            print("[ERRO] Nenhum ar condicionado configurado.")
            return

        for sid in sensors:
            print(f"[Sensor de Presenca] Sala vazia")
            self._trigger_sensor(sid, {"EMPTY": 1})
            time.sleep(0.5)
        print("[Sala] Aguardando timer de desligamento...")
        time.sleep(6)

    def _req_3_4(self):
        print("\n" + "="*60)
        print("REQUISITO 3.4 - Ligar chave do projetor")
        print("="*60)
        print()

        sensors = self._select_sensors(2)
        if not sensors:
            print("[ERRO] Nenhuma chave do projetor configurada.")
            return
        if not self._tem_atuador(4):
            print("[ERRO] Nenhum sistema de iluminacao configurado.")
            return
        if not self._tem_atuador(5):
            print("[ERRO] Nenhum projetor configurado.")
            return

        for sid in sensors:
            print(f"[Chave do Projetor] Ligou a chave")
            self._trigger_sensor(sid, {"STATUS": 1})
            time.sleep(0.5)
        time.sleep(2)

    def _req_3_5(self):
        print("\n" + "="*60)
        print("REQUISITO 3.5 - Desligar chave do projetor")
        print("="*60)
        print()

        sensors = self._select_sensors(2)
        if not sensors:
            print("[ERRO] Nenhuma chave do projetor configurada.")
            return
        if not self._tem_atuador(4):
            print("[ERRO] Nenhum sistema de iluminacao configurado.")
            return
        if not self._tem_atuador(5):
            print("[ERRO] Nenhum projetor configurado.")
            return

        for sid in sensors:
            print(f"[Chave do Projetor] Desligou a chave")
            self._trigger_sensor(sid, {"STATUS": 0})
            time.sleep(0.5)
        time.sleep(2)

    def _req_3_6(self):
        print("\n" + "="*60)
        print("REQUISITO 3.6 - Registro de presenca do aluno")
        print("="*60)
        print()

        sensors = self._select_sensors(1)
        if not sensors:
            print("[ERRO] Nenhum leitor de cartao configurado.")
            return

        print("Alunos disponiveis:")
        for i, (nome, nro) in enumerate(STUDENTS, 1):
            print(f"  {i}. {nome} ({nro})")

        try:
            idx = int(input("\nEscolha o aluno (1-5): ")) - 1
            if not (0 <= idx < len(STUDENTS)):
                print("Opcao invalida, usando primeiro aluno.")
                idx = 0
        except ValueError:
            print("Entrada invalida, usando primeiro aluno.")
            idx = 0

        nome, nro = STUDENTS[idx]
        nome_input = input(f"Nome (Enter para '{nome}'): ").strip()
        nro_input = input(f"Numero (Enter para '{nro}'): ").strip()
        nome_final = nome_input if nome_input else nome
        nro_final = nro_input if nro_input else nro

        for sid in sensors:
            self._trigger_sensor(sid, {"NROALUNO": nro_final, "NOME": nome_final})
            print(f"\n[Leitor de Cartao] Aluno {nome_final} ({nro_final}) bateu o cartao")
            time.sleep(0.5)

    def _req_3_7(self):
        print("\n" + "="*60)
        print("REQUISITO 3.7 - Rotina de encerramento as 23h")
        print("="*60)
        print()

        if not self._tem_atuador(4):
            print("[ERRO] Nenhum sistema de iluminacao configurado.")
            return
        if not self._tem_atuador(5):
            print("[ERRO] Nenhum projetor configurado.")
            return
        if not self._tem_atuador(6):
            print("[ERRO] Nenhum ar condicionado configurado.")
            return

        print("[Gerenciador] Relogio interno marcou 23:00")
        print()

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((GERENCIADOR, GERENCIADOR_PORT_TCP))
            local_addr = sock.getsockname()
            header = Header(12, local_addr, (GERENCIADOR, GERENCIADOR_PORT_TCP), HEADER_SIZE).to_bytes()
            sock.sendall(header)
            sock.close()
        except Exception as e:
            print(f"[ERRO] Falha ao comunicar com o gerenciador: {e}")
            return

        time.sleep(3)

    def _req_3_8(self):
        print("\n" + "="*60)
        print("REQUISITO 3.8/4 - Cliente requisita lista de presenca")
        print("="*60)
        print()

        if not self.has_client or not self.client_queue:
            print("[ERRO] Cliente nao configurado.")
            return

        data = input("Data (YYYY-MM-DD) [Enter para hoje]: ").strip()
        if not data:
            data = time.strftime("%Y-%m-%d")

        self.client_queue.put({"action": "request_data", "date": data})
        print(f"\n[Cliente] Requisitou lista de presenca para {data}")
        time.sleep(0.5)

    def requirement_simulation(self):
        """Menu interativo para executar cada requisito funcional."""
        print("\n============================================================")
        print("         SIMULACAO DOS REQUISITOS FUNCIONAIS")
        print("============================================================")

        while True:
            print()
            print("  1.  Req 3.2 - Sensor de presenca detecta pessoas na sala")
            print("  2.  Req 3.3 - Sensor de presenca detecta sala vazia")
            print("  3.  Req 3.4 - Ligar chave do projetor")
            print("  4.  Req 3.5 - Desligar chave do projetor")
            print("  5.  Req 3.6 - Leitor de cartao registra presenca")
            print("  6.  Req 3.7 - Rotina de encerramento as 23h")
            print("  7.  Req 3.8 - Cliente requisita lista de presenca")
            print("  0.  Sair")
            print()

            try:
                escolha = int(input("Escolha um requisito: "))
            except ValueError:
                print("Entrada invalida.")
                continue

            print()

            if escolha == 1:
                self._req_3_2()
            elif escolha == 2:
                self._req_3_3()
            elif escolha == 3:
                self._req_3_4()
            elif escolha == 4:
                self._req_3_5()
            elif escolha == 5:
                self._req_3_6()
            elif escolha == 6:
                self._req_3_7()
            elif escolha == 7:
                self._req_3_8()
            elif escolha == 0:
                print("Encerrando simulacao.")
                break
            else:
                print("Opcao invalida.")
                continue

            input("\nPressione Enter para continuar...")

    def run(self):
        """Ponto de entrada: configura, inicializa e executa a simulacao."""
        try:
            self.setup_interactive()
            self.start_all()
            self.requirement_simulation()

        except KeyboardInterrupt:
            print("\n\nSimulacao interrompida pelo usuario.")
        finally:
            self.cleanup()


if __name__ == "__main__":
    sala = SalaDeAula()
    sala.run()

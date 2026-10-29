"""
A sessão Spark do app: criada uma vez e reaproveitada.

A sessão do Glue é a parte cara (leva de 30 a 90 s para subir e cobra enquanto está ativa). Ela fica
guardada no processo do app, e não na aba do navegador: recarregar a página ou abrir outra aba
continua usando a mesma sessão.

- Antes de cada execução o status é conferido. Se a sessão caiu (tempo sem uso, erro), outra é
  criada na hora. Um status READY conferido há poucos segundos dispensa nova consulta.
- Se ela cair entre a conferência e o envio da query, a AWS recusa o envio: a ferramenta cria outra
  sessão e reenvia (só quando o envio foi recusado, para nunca rodar a mesma query duas vezes).
- Se o app foi reiniciado, uma sessão sua criada antes, com a mesma configuração e ainda ativa, é
  reaproveitada em vez de pagar outra.
- A sessão pode ser iniciada em segundo plano: dá para continuar editando a query enquanto ela sobe.
"""
from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from engines import TERMINAL_SESSION_STATES, EngineError, poll_intervals

StatusCallback = Callable[[str], None]

FRESH_S = 15                                     # READY conferido há menos que isso vale sem nova consulta
STATUS_TTL = {"READY": 20, "PROVISIONING": 4}    # cache do status para desenhar a tela
SESSION_LOST_ERRORS = {"IllegalSessionStateException", "EntityNotFoundException"}
BUSY_ERROR = "SessionBusyException"
BUSY_WAIT_S = 60


@dataclass
class Slot:
    """O que o app sabe da sessão de uma conta/região (ou do Spark local)."""
    session_id: str | None = None
    fingerprint: str | None = None       # configuração usada para criar a sessão
    status: str | None = None
    checked_at: float = 0.0              # quando o status foi consultado
    ready_since: float = 0.0
    last_used: float = 0.0               # fim da última execução
    idle_timeout_min: int | None = None
    running: dict | None = None          # {"session": id, "id": n} enquanto uma query roda
    starting: bool = False               # iniciando em segundo plano
    progress: str = ""                   # última mensagem do início em segundo plano
    error: str | None = None             # erro do início em segundo plano
    adopted: str | None = None           # sessão anterior reaproveitada automaticamente
    reusable: list[dict] | None = None   # sessões anteriores suas (None = ainda não procurou)
    searching: bool = False              # procurando sessões anteriores em segundo plano
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def expected_end(self) -> float | None:
        """Quando a sessão deve encerrar sozinha se ninguém usar (estimativa)."""
        if not self.session_id or not self.idle_timeout_min:
            return None
        base = max(self.last_used, self.ready_since)
        return base + self.idle_timeout_min * 60 if base else None


def engine_key(engine) -> tuple:
    if getattr(engine, "kind", "glue") == "local":
        return ("local",)
    return ("glue",) + tuple(engine.account.key())


class SessionHub:
    def __init__(self):
        self._slots: dict[tuple, Slot] = {}
        self._lock = threading.Lock()

    def slot(self, engine) -> Slot:
        chave = engine_key(engine)
        with self._lock:
            return self._slots.setdefault(chave, Slot())

    def reset(self):
        with self._lock:
            self._slots.clear()

    # ------------------------------------------------------------------ estado
    @staticmethod
    def _mark(slot: Slot, status: str | None):
        slot.status, slot.checked_at = status, time.time()
        if status == "READY" and not slot.ready_since:
            slot.ready_since = slot.checked_at
        if status is None or status in TERMINAL_SESSION_STATES:
            slot.session_id = slot.fingerprint = None
            slot.ready_since = slot.last_used = 0.0
            slot.running = slot.adopted = None

    def _set(self, slot: Slot, session_id: str, fingerprint: str | None, idle: int | None, status: str):
        slot.session_id, slot.fingerprint, slot.idle_timeout_min = session_id, fingerprint, idle
        slot.ready_since = slot.last_used = 0.0
        slot.error = None
        self._mark(slot, status)

    def status(self, engine) -> str | None:
        """Status para mostrar na tela: consulta a AWS no máximo a cada poucos segundos."""
        slot = self.slot(engine)
        if engine.kind == "local":
            return engine.session_status()
        if not slot.session_id:
            return None
        if time.time() - slot.checked_at < STATUS_TTL.get(slot.status, 0):
            return slot.status
        status = engine.session_status(slot.session_id)
        self._mark(slot, status)
        return status

    # ------------------------------------------------------------------ garantir uma sessão pronta
    def ensure(self, engine, on_status: StatusCallback = lambda m: None, fingerprint: str | None = None) -> str:
        """Devolve uma sessão pronta: a atual (conferida), uma anterior equivalente ou uma nova."""
        if engine.kind == "local":
            return engine.ensure_session(on_status)
        slot = self.slot(engine)
        # outra execução (ou o início em segundo plano) pode estar criando a sessão: espera por ela
        while not slot.lock.acquire(timeout=0.5):
            on_status(slot.progress or "Aguardando a sessão que já está iniciando…")
        try:
            return self._ensure_locked(engine, slot, on_status, fingerprint)
        finally:
            slot.lock.release()

    def _ensure_locked(self, engine, slot: Slot, on_status: StatusCallback, fingerprint: str | None) -> str:
        if slot.session_id:
            engine.session_id = slot.session_id
            if slot.status == "READY" and time.time() - slot.checked_at < FRESH_S:
                return slot.session_id
            status = engine.session_status()
            self._mark(slot, status)
            if status == "READY":
                return slot.session_id
            if slot.session_id:                                  # ainda subindo
                on_status(f"Sessão {slot.session_id} ainda está iniciando…")
                engine.wait_ready(on_status)
                self._mark(slot, "READY")
                return slot.session_id
            on_status("A sessão anterior encerrou (tempo sem uso ou erro); criando outra…")
            engine.session_id = None

        anterior = self._previous_session(engine, slot, fingerprint)
        if anterior is not None:
            self._set(slot, anterior["Id"], anterior.get("fingerprint"), anterior.get("IdleTimeout"),
                      anterior.get("Status"))
            slot.adopted = anterior["Id"]
            engine.session_id = anterior["Id"]
            on_status(f"Reaproveitando a sessão {anterior['Id']}, criada antes com a mesma configuração…")
            if slot.status != "READY":
                engine.wait_ready(on_status)
                self._mark(slot, "READY")
            return anterior["Id"]

        def criada(sid: str):
            self._set(slot, sid, fingerprint, int(engine.instance.idle_timeout_min), "PROVISIONING")

        sid = engine.start(on_status, on_created=criada)
        if slot.session_id != sid:          # start sem on_created (ex.: motor falso nos testes)
            criada(sid)
        self._mark(slot, "READY")
        return sid

    @staticmethod
    def _previous_session(engine, slot: Slot, fingerprint: str | None) -> dict | None:
        """Sessão sua, criada antes por esta ferramenta, com a mesma configuração e ainda ativa."""
        if not fingerprint:
            return None
        try:
            candidatas = engine.reusable_sessions()
        except Exception:
            return None
        slot.reusable = [c for c in candidatas if c.get("fingerprint") != fingerprint]
        return next((c for c in candidatas if c.get("fingerprint") == fingerprint), None)

    # ------------------------------------------------------------------ segundo plano
    def start_background(self, engine, fingerprint: str | None = None) -> bool:
        """Inicia (ou confere) a sessão numa thread. Devolve False se já havia algo em andamento."""
        slot = self.slot(engine)
        with self._lock:
            if slot.starting:
                return False
            slot.starting, slot.error, slot.progress = True, None, "Iniciando a sessão…"
        motor = copy.copy(engine)          # o objeto da tela continua livre; os clientes AWS são os mesmos

        def trabalho():
            try:
                self.ensure(motor, lambda m: setattr(slot, "progress", m), fingerprint)
            except Exception as e:          # vai para a tela
                slot.error = f"{type(e).__name__}: {e}" if not isinstance(e, EngineError) else str(e)
            finally:
                slot.starting = False

        threading.Thread(target=trabalho, name="sqs-sessao", daemon=True).start()
        return True

    def find_reusable(self, engine):
        """Procura, em segundo plano, sessões suas que ainda estão ativas (uma vez por conta/região)."""
        slot = self.slot(engine)
        with self._lock:
            if engine.kind != "glue" or slot.reusable is not None or slot.searching:
                return
            slot.searching = True
        motor = copy.copy(engine)

        def trabalho():
            try:
                encontradas = motor.reusable_sessions()
            except Exception:
                encontradas = []
            slot.reusable = [s for s in encontradas if s.get("Id") != slot.session_id]
            slot.searching = False

        threading.Thread(target=trabalho, name="sqs-reaproveitar", daemon=True).start()

    def adopt(self, engine, session: dict):
        """Passa a usar uma sessão anterior escolhida na tela."""
        slot = self.slot(engine)
        with slot.lock:
            self._set(slot, session["Id"], session.get("fingerprint"), session.get("IdleTimeout"),
                      session.get("Status"))
            slot.reusable = [s for s in (slot.reusable or []) if s.get("Id") != session["Id"]]

    # ------------------------------------------------------------------ execução
    def run(self, engine, code: str, on_status: StatusCallback = lambda m: None,
            fingerprint: str | None = None, should_cancel: Callable[[], bool] = lambda: False) -> dict:
        slot = self.slot(engine)
        self.ensure(engine, on_status, fingerprint)

        # uma execução interrompida (você mexeu na tela no meio) pode ter deixado a query anterior
        # rodando; ela atrasaria esta, então é cancelada
        anterior = slot.running
        if anterior and anterior.get("session") == engine.session_id:
            if engine.cancel_statement(anterior["id"]):
                on_status("A execução anterior, que tinha ficado rodando, foi cancelada")
        slot.running = None

        enviado = []

        def guardar(statement_id: int):
            enviado.append(statement_id)
            slot.running = {"session": engine.session_id, "id": statement_id}

        espera_ocupada = time.time()
        intervalos = poll_intervals(1.0, 5.0)
        tentativas_sessao = 0
        while True:
            try:
                res = engine.run(code, on_status, should_cancel=should_cancel, on_submitted=guardar)
                break
            except Exception as e:
                nome = type(e).__name__
                if enviado or engine.kind == "local":
                    raise                               # a query chegou a rodar: não reenvia
                if nome in SESSION_LOST_ERRORS and tentativas_sessao == 0:
                    tentativas_sessao += 1
                    on_status("A sessão encerrou antes de receber a query; criando outra…")
                    with slot.lock:
                        self._mark(slot, None)
                    engine.session_id = None
                    self.ensure(engine, on_status, fingerprint)
                    continue
                if nome == BUSY_ERROR and time.time() - espera_ocupada < BUSY_WAIT_S:
                    on_status("A sessão está ocupada com outra execução; aguardando…")
                    time.sleep(next(intervalos))
                    continue
                if nome == BUSY_ERROR:
                    raise EngineError("A sessão continua ocupada com outra execução. Espere ela terminar "
                                      "ou encerre a sessão.") from e
                raise
        slot.running = None
        if engine.kind == "glue":
            slot.last_used = time.time()
            self._mark(slot, "READY")
        return res

    def cancel_running(self, engine) -> bool:
        """Cancela a query que ficou rodando (botão Cancelar). Devolve True se havia o que cancelar."""
        slot = self.slot(engine)
        atual = slot.running
        if not atual:
            return False
        engine.session_id = atual["session"]
        try:
            return engine.cancel_statement(atual["id"])
        finally:
            slot.running = None

    def stop(self, engine, session_id: str | None = None):
        """Encerra a sessão atual (ou a indicada, ex.: uma anterior oferecida para reaproveitar)."""
        slot = self.slot(engine)
        alvo = session_id or slot.session_id
        if engine.kind == "local":
            engine.stop()
            return
        if alvo:
            engine.stop(alvo)
        with slot.lock:
            if alvo == slot.session_id:
                self._mark(slot, None)
            slot.reusable = [s for s in (slot.reusable or []) if s.get("Id") != alvo]


HUB = SessionHub()

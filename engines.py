"""
Motores de execução Spark.

- GlueSessionEngine: AWS Glue Interactive Sessions (Spark gerenciado, mesmo mecanismo do
  kernel PySpark do Glue). Você escolhe tipo de worker (G.1X, G.2X...) e quantidade.
- LocalSparkEngine: Spark local, para testar a ferramenta sem custo na AWS.
"""
from __future__ import annotations

import io
import time
import uuid
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from typing import Callable

from code_builder import parse_result

StatusCallback = Callable[[str], None]
IdCallback = Callable[[str], None]

# Estados finais de uma sessão Glue: depois deles a sessão não volta a ficar READY
TERMINAL_SESSION_STATES = {"FAILED", "TIMEOUT", "STOPPED", "STOPPING"}
# Estados de um statement que ainda está na fila ou rodando
ACTIVE_STATEMENT_STATES = {"WAITING", "RUNNING"}

@dataclass(frozen=True)
class WorkerType:
    vcpu: int
    memoria_gb: int
    dpu: int
    nota: str = ""

    def descricao(self) -> str:
        texto = f"{self.vcpu} vCPU / {self.memoria_gb} GB · {self.dpu} DPU"
        return f"{texto} ({self.nota})" if self.nota else texto


# Tipos de worker do Glue para jobs/sessões Spark (o G.025X é só para jobs de streaming)
_DEMORA = "pode demorar mais para iniciar"
GLUE_WORKER_TYPES = {
    "G.1X": WorkerType(4, 16, 1),
    "G.2X": WorkerType(8, 32, 2),
    "G.4X": WorkerType(16, 64, 4),
    "G.8X": WorkerType(32, 128, 8),
    "G.12X": WorkerType(48, 192, 12, _DEMORA),
    "G.16X": WorkerType(64, 256, 16, _DEMORA),
    "R.1X": WorkerType(4, 32, 1, "memória otimizada; " + _DEMORA),
    "R.2X": WorkerType(8, 64, 2, "memória otimizada; " + _DEMORA),
    "R.4X": WorkerType(16, 128, 4, "memória otimizada; " + _DEMORA),
    "R.8X": WorkerType(32, 256, 8, "memória otimizada; " + _DEMORA),
}
DEFAULT_WORKER_TYPE = "G.1X"

# Versões do Glue com Spark e Python de cada uma
GLUE_VERSIONS = {
    "6.0": "Spark 4.1 · Python 3.13",
    "5.1": "Spark 3.5.6 · Python 3.11",
    "5.0": "Spark 3.5.4 · Python 3.11",
    "4.0": "Spark 3.3 · Python 3.10",
}
DEFAULT_GLUE_VERSION = "5.0"
GLUE_VERSION_NOTES = {
    "6.0": ("O Glue 6.0 usa Spark 4.1 com o modo ANSI ligado: overflow, divisão por zero e cast inválido "
            "viram erro em vez de NULL. Se a produção roda Glue 5.x, teste com 5.x."),
}

# Tag com quem criou a sessão: permite reencontrar as próprias sessões depois de recarregar a página
OWNER_TAG = "spark-query-studio-owner"
SESSION_PREFIX = "sqs-"


@dataclass
class AwsAccount:
    region: str = "us-east-1"
    auth_mode: str = "profile"        # "profile" | "keys" | "default"
    profile: str = ""
    access_key: str = ""
    secret_key: str = ""
    session_token: str = ""
    glue_role_arn: str = ""           # role que o Glue assume para rodar a sessão
    temp_dir: str = ""                # s3://bucket/tmp/ (opcional)

    def boto_session(self):
        import boto3
        if self.auth_mode == "profile" and self.profile:
            return boto3.Session(profile_name=self.profile, region_name=self.region)
        if self.auth_mode == "keys" and self.access_key and self.secret_key:
            return boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                aws_session_token=self.session_token or None,
                region_name=self.region,
            )
        return boto3.Session(region_name=self.region)


@dataclass
class InstanceConfig:
    worker_type: str = DEFAULT_WORKER_TYPE
    num_workers: int = 2
    glue_version: str = DEFAULT_GLUE_VERSION
    idle_timeout_min: int = 30
    extra_args: dict[str, str] = field(default_factory=dict)   # DefaultArguments extras
    tags: dict[str, str] = field(default_factory=dict)


class EngineError(RuntimeError):
    pass


# --------------------------------------------------------------------------------------
# AWS Glue Interactive Sessions
# --------------------------------------------------------------------------------------
class GlueSessionEngine:
    READY_TIMEOUT_S = 15 * 60
    POLL_S = 3

    def __init__(self, account: AwsAccount, instance: InstanceConfig, glue_client=None, sts_client=None):
        self.account = account
        self.instance = instance
        # glue_client / sts_client permitem reaproveitar clientes (cache) ou injetar falsos nos testes
        self._glue = glue_client if glue_client is not None else account.boto_session().client("glue")
        self._sts = sts_client
        self._caller_arn: str | None = None
        self.session_id: str | None = None

    # ---------- conta ----------
    def whoami(self) -> dict:
        if self._sts is None:
            self._sts = self.account.boto_session().client("sts")
        return self._sts.get_caller_identity()

    def caller_arn(self) -> str | None:
        """ARN de quem está usando (para marcar as sessões). None se não der para descobrir."""
        if self._caller_arn is None:
            try:
                self._caller_arn = self.whoami()["Arn"]
            except Exception:
                return None
        return self._caller_arn

    def reusable_sessions(self) -> list[dict]:
        """Sessões desta ferramenta, criadas por você, que ainda estão ativas (mais recente primeiro).

        Servem para continuar usando a sessão depois de recarregar a página, em vez de pagar outra.
        """
        arn = self.caller_arn()
        if not arn:
            return []
        sessoes, token = [], None
        while True:
            kw = {"Tags": {OWNER_TAG: arn}, "MaxResults": 100}
            if token:
                kw["NextToken"] = token
            resp = self._glue.list_sessions(**kw)
            sessoes += resp.get("Sessions", [])
            token = resp.get("NextToken")
            if not token:
                break
        ativas = [s for s in sessoes
                  if s.get("Id", "").startswith(SESSION_PREFIX) and s.get("Status") in ("READY", "PROVISIONING")]
        return sorted(ativas, key=lambda s: str(s.get("CreatedOn", "")), reverse=True)

    # ---------- sessão ----------
    def session_status(self, session_id: str | None = None) -> str | None:
        sid = session_id or self.session_id
        if not sid:
            return None
        try:
            return self._glue.get_session(Id=sid)["Session"]["Status"]
        except Exception as e:  # EntityNotFoundException: sessão não existe (mais)
            if type(e).__name__ == "EntityNotFoundException":
                return None
            raise

    def attach(self, session_id: str) -> bool:
        """Reaproveita uma sessão existente se ainda estiver READY."""
        if self.session_status(session_id) == "READY":
            self.session_id = session_id
            return True
        return False

    def start(self, on_status: StatusCallback = lambda s: None, on_created: IdCallback = lambda sid: None) -> str:
        """Cria uma sessão e espera ficar READY.

        on_created é chamado logo depois da criação, antes da espera: se a espera for interrompida
        (ex.: o Streamlit reinicia o script porque você mexeu na tela), quem chamou já guardou o ID
        e pode continuar esperando a mesma sessão em vez de criar outra.
        """
        if not self.account.glue_role_arn:
            raise EngineError("Informe o ARN da IAM Role do Glue em 'Configurar conta AWS'.")

        inst = self.instance
        # O Spark UI não vem ligado: em sessões interativas ele só funciona com --spark-event-logs-path
        # (um caminho no S3) e um Spark History Server. Dá para ligar em "Argumentos extras".
        args = {
            "--enable-glue-datacatalog": "true",   # permite ler as mesmas tabelas do Athena
        }
        if self.account.temp_dir:
            args["--TempDir"] = self.account.temp_dir
        args.update({k: v for k, v in inst.extra_args.items() if k})

        sid = f"{SESSION_PREFIX}{uuid.uuid4().hex[:12]}"
        params = dict(
            Id=sid,
            Role=self.account.glue_role_arn,
            Command={"Name": "glueetl", "PythonVersion": "3"},
            GlueVersion=inst.glue_version,
            WorkerType=inst.worker_type,
            NumberOfWorkers=int(inst.num_workers),
            IdleTimeout=int(inst.idle_timeout_min),
            DefaultArguments=args,
            Description="Spark Query Studio",
        )
        tags = dict(inst.tags)
        arn = self.caller_arn()
        if arn:
            tags[OWNER_TAG] = arn[:256]
        if tags:
            params["Tags"] = tags

        on_status(f"Criando sessão {sid} ({inst.num_workers} x {inst.worker_type}, Glue {inst.glue_version})…")
        try:
            self._glue.create_session(**params)
        except Exception as e:
            # Sem permissão para tags (glue:TagResource): cria sem a tag de dono, em vez de falhar.
            # A sessão só não aparece em "reaproveitar" depois de recarregar a página.
            sem_permissao = type(e).__name__ == "AccessDeniedException" and "tag" in str(e).lower()
            if not (sem_permissao and OWNER_TAG in params.get("Tags", {})):
                raise
            params["Tags"] = {k: v for k, v in params["Tags"].items() if k != OWNER_TAG}
            if not params["Tags"]:
                params.pop("Tags")
            self._glue.create_session(**params)
        self.session_id = sid
        on_created(sid)
        return self.wait_ready(on_status)

    def wait_ready(self, on_status: StatusCallback = lambda s: None) -> str:
        """Espera a sessão atual ficar READY (ou falha com EngineError)."""
        sid = self.session_id
        t0 = time.time()
        not_found = 0
        while True:
            status = self.session_status()
            on_status(f"Sessão {sid}: {status} ({int(time.time() - t0)}s)")
            if status == "READY":
                return sid
            if status is None and not_found < 3:
                not_found += 1          # logo após o create_session a sessão pode não aparecer ainda
                time.sleep(self.POLL_S)
                continue
            if status in TERMINAL_SESSION_STATES or status is None:
                detail = self._glue.get_session(Id=sid)["Session"].get("ErrorMessage", "") if status else ""
                raise EngineError(f"Sessão terminou com status {status}. {detail}")
            if time.time() - t0 > self.READY_TIMEOUT_S:
                raise EngineError("Tempo esgotado esperando a sessão ficar READY.")
            time.sleep(self.POLL_S)

    def ensure_session(self, on_status: StatusCallback = lambda s: None, on_created: IdCallback = lambda sid: None) -> str:
        """Garante uma sessão READY: reaproveita a atual, espera se ela ainda está subindo, ou cria outra."""
        if self.session_id:
            status = self.session_status()
            if status == "READY":
                return self.session_id
            if status is not None and status not in TERMINAL_SESSION_STATES:   # PROVISIONING
                on_status(f"Sessão {self.session_id} ainda está iniciando…")
                return self.wait_ready(on_status)
            self.session_id = None                          # encerrada ou inexistente: cria outra
        return self.start(on_status, on_created)

    def stop(self, session_id: str | None = None):
        """Encerra a sessão atual (ou a indicada). Erros (ex.: sem permissão) sobem para quem chamou mostrar."""
        sid = session_id or self.session_id
        if sid:
            try:
                self._glue.stop_session(Id=sid)
            except Exception as e:
                if type(e).__name__ != "EntityNotFoundException":
                    raise
            if sid == self.session_id:
                self.session_id = None

    # ---------- statements ----------
    def statement_state(self, statement_id: int) -> str | None:
        try:
            return self._glue.get_statement(SessionId=self.session_id, Id=statement_id)["Statement"]["State"]
        except Exception as e:
            if type(e).__name__ == "EntityNotFoundException":
                return None
            raise

    def cancel_statement(self, statement_id: int, wait_s: float = 30) -> bool:
        """Cancela um statement que ainda está na fila ou rodando. Devolve True se precisou cancelar."""
        if not self.session_id or self.statement_state(statement_id) not in ACTIVE_STATEMENT_STATES:
            return False
        self._glue.cancel_statement(SessionId=self.session_id, Id=statement_id)
        t0 = time.time()
        while time.time() - t0 < wait_s:
            if self.statement_state(statement_id) not in ACTIVE_STATEMENT_STATES | {"CANCELLING"}:
                break
            time.sleep(min(self.POLL_S, 1))
        return True

    # ---------- execução ----------
    def run(self, code: str, on_status: StatusCallback = lambda s: None,
            should_cancel: Callable[[], bool] = lambda: False,
            on_submitted: Callable[[int], None] = lambda stmt_id: None) -> dict:
        """Roda o código na sessão. on_submitted recebe o ID do statement logo depois do envio,
        para que uma execução interrompida possa ser cancelada depois."""
        if not self.session_id:
            self.start(on_status)

        stmt_id = self._glue.run_statement(SessionId=self.session_id, Code=code)["Id"]
        on_submitted(stmt_id)
        t0 = time.time()
        while True:
            st = self._glue.get_statement(SessionId=self.session_id, Id=stmt_id)["Statement"]
            state = st["State"]
            progress = st.get("Progress")
            pct = f" {progress * 100:.0f}%" if isinstance(progress, (int, float)) and progress else ""
            on_status(f"Query: {state}{pct} ({int(time.time() - t0)}s)")

            if state == "AVAILABLE":
                out = st.get("Output", {}) or {}
                if out.get("Status") == "ERROR":
                    tb = "\n".join(out.get("Traceback", []) or [])
                    return {"ok": False, "error": f"{out.get('ErrorName')}: {out.get('ErrorValue')}",
                            "traceback": tb, "elapsed": round(time.time() - t0, 2), "log": []}
                text = (out.get("Data") or {}).get("TextPlain", "")
                result = parse_result(text)
                if result is None:
                    return {"ok": False, "error": "Não foi possível ler o resultado da sessão.",
                            "traceback": text[-4000:], "elapsed": round(time.time() - t0, 2), "log": []}
                return result
            if state in ("ERROR", "CANCELLED"):
                return {"ok": False, "error": f"Statement terminou como {state}", "traceback": "",
                        "elapsed": round(time.time() - t0, 2), "log": []}
            if should_cancel():
                self._glue.cancel_statement(SessionId=self.session_id, Id=stmt_id)
            time.sleep(self.POLL_S)


# --------------------------------------------------------------------------------------
# Spark local (teste sem AWS)
# --------------------------------------------------------------------------------------
class LocalSparkEngine:
    _spark = None

    def __init__(self, instance: InstanceConfig | None = None, account: AwsAccount | None = None):
        self.instance = instance or InstanceConfig()
        self.account = account
        self.session_id = "local"

    def start(self, on_status: StatusCallback = lambda s: None, on_created: IdCallback = lambda sid: None) -> str:
        if LocalSparkEngine._spark is None:
            on_status("Iniciando Spark local…")
            from pyspark.sql import SparkSession
            builder = (SparkSession.builder.appName("SparkQueryStudio")
                       .master("local[*]")
                       .config("spark.ui.showConsoleProgress", "false"))
            # Para ler s3a:// localmente é preciso o hadoop-aws no classpath:
            #   .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            for k, v in self.instance.extra_args.items():
                if k.startswith("spark."):
                    builder = builder.config(k, v)
            LocalSparkEngine._spark = builder.getOrCreate()
        return "local"

    def session_status(self, *_):
        return "READY" if LocalSparkEngine._spark is not None else None

    def ensure_session(self, on_status: StatusCallback = lambda s: None, on_created: IdCallback = lambda sid: None) -> str:
        return self.start(on_status, on_created)

    def cancel_statement(self, *_args, **_kw) -> bool:
        return False

    def attach(self, *_):
        return LocalSparkEngine._spark is not None

    def stop(self):
        if LocalSparkEngine._spark is not None:
            LocalSparkEngine._spark.stop()
            LocalSparkEngine._spark = None

    def run(self, code: str, on_status: StatusCallback = lambda s: None, **_) -> dict:
        self.start(on_status)
        on_status("Executando query no Spark local…")
        buf = io.StringIO()
        with redirect_stdout(buf):
            exec(code, {"spark": LocalSparkEngine._spark})
        result = parse_result(buf.getvalue())
        if result is None:
            return {"ok": False, "error": "Sem resultado.", "traceback": buf.getvalue()[-4000:], "log": []}
        return result

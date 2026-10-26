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

# Tipos de worker do Glue (vCPU / memória por worker)
GLUE_WORKER_TYPES = {
    "G.025X": "2 vCPU / 4 GB (apenas streaming)",
    "G.1X": "4 vCPU / 16 GB",
    "G.2X": "8 vCPU / 32 GB",
    "G.4X": "16 vCPU / 64 GB",
    "G.8X": "32 vCPU / 128 GB",
    "R.1X": "4 vCPU / 32 GB (memória otimizada)",
    "R.2X": "8 vCPU / 64 GB (memória otimizada)",
}
GLUE_VERSIONS = ["5.0", "4.0"]


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
    worker_type: str = "G.1X"
    num_workers: int = 2
    glue_version: str = "5.0"
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

    def __init__(self, account: AwsAccount, instance: InstanceConfig):
        self.account = account
        self.instance = instance
        self._glue = account.boto_session().client("glue")
        self.session_id: str | None = None

    # ---------- conta ----------
    def whoami(self) -> dict:
        sts = self.account.boto_session().client("sts")
        return sts.get_caller_identity()

    # ---------- sessão ----------
    def session_status(self, session_id: str | None = None) -> str | None:
        sid = session_id or self.session_id
        if not sid:
            return None
        try:
            return self._glue.get_session(Id=sid)["Session"]["Status"]
        except self._glue.exceptions.EntityNotFoundException:
            return None

    def attach(self, session_id: str) -> bool:
        """Reaproveita uma sessão existente se ainda estiver READY."""
        if self.session_status(session_id) == "READY":
            self.session_id = session_id
            return True
        return False

    def start(self, on_status: StatusCallback = lambda s: None) -> str:
        if not self.account.glue_role_arn:
            raise EngineError("Informe o ARN da IAM Role do Glue em 'Configurar conta AWS'.")

        inst = self.instance
        args = {
            "--enable-glue-datacatalog": "true",   # permite ler as mesmas tabelas do Athena
            "--enable-spark-ui": "true",
        }
        if self.account.temp_dir:
            args["--TempDir"] = self.account.temp_dir
        args.update({k: v for k, v in inst.extra_args.items() if k})

        sid = f"sqs-{uuid.uuid4().hex[:12]}"
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
        if inst.tags:
            params["Tags"] = inst.tags

        on_status(f"Criando sessão {sid} ({inst.num_workers} x {inst.worker_type}, Glue {inst.glue_version})…")
        self._glue.create_session(**params)
        self.session_id = sid

        t0 = time.time()
        while True:
            status = self.session_status()
            on_status(f"Sessão {sid}: {status} ({int(time.time() - t0)}s)")
            if status == "READY":
                return sid
            if status in ("FAILED", "TIMEOUT", "STOPPED", "STOPPING", None):
                detail = self._glue.get_session(Id=sid)["Session"].get("ErrorMessage", "") if status else ""
                raise EngineError(f"Sessão terminou com status {status}. {detail}")
            if time.time() - t0 > self.READY_TIMEOUT_S:
                raise EngineError("Tempo esgotado esperando a sessão ficar READY.")
            time.sleep(self.POLL_S)

    def stop(self):
        if self.session_id:
            try:
                self._glue.stop_session(Id=self.session_id)
            except Exception:
                pass
            self.session_id = None

    # ---------- execução ----------
    def run(self, code: str, on_status: StatusCallback = lambda s: None,
            should_cancel: Callable[[], bool] = lambda: False) -> dict:
        if not self.session_id:
            self.start(on_status)

        stmt_id = self._glue.run_statement(SessionId=self.session_id, Code=code)["Id"]
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
            time.sleep(2)


# --------------------------------------------------------------------------------------
# Spark local (teste sem AWS)
# --------------------------------------------------------------------------------------
class LocalSparkEngine:
    _spark = None

    def __init__(self, instance: InstanceConfig | None = None, account: AwsAccount | None = None):
        self.instance = instance or InstanceConfig()
        self.account = account
        self.session_id = "local"

    def start(self, on_status: StatusCallback = lambda s: None) -> str:
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

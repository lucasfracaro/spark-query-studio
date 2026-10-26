"""
Gera o código PySpark que será executado no motor (Glue Interactive Session ou Spark local).

A query e as configurações viajam em base64 dentro do código gerado, para não haver
problemas com aspas, quebras de linha ou caracteres especiais.
O resultado volta como uma linha marcada com RESULT_MARKER contendo JSON em base64.
"""
from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field, asdict

RESULT_MARKER = "__SQS_RESULT__"
DEFAULT_LIMIT = 100


@dataclass
class InputSource:
    """Uma entrada de dados que vira uma temp view no Spark."""
    alias: str                 # nome da view usada na query
    kind: str = "catalog"      # "catalog" (Glue Data Catalog / Athena) ou "s3"
    source: str = ""           # "database.tabela" ou "s3://bucket/caminho/"
    fmt: str = "parquet"       # parquet | csv | json | orc | delta | iceberg
    options: str = ""          # "header=true;sep=;" (só para s3)
    filter: str = ""           # filtro opcional aplicado antes de registrar (ex: "dt >= '2026-10-01'")
    cache: bool = False        # cachear a view

    def is_valid(self) -> bool:
        return bool(self.alias and self.alias.strip() and self.source and self.source.strip())


@dataclass
class Orchestration:
    inputs: list[InputSource] = field(default_factory=list)
    spark_conf: dict[str, str] = field(default_factory=dict)
    default_database: str = ""
    limit: int = DEFAULT_LIMIT


def _b64(obj) -> str:
    return base64.b64encode(json.dumps(obj).encode("utf-8")).decode("ascii")


def split_statements(sql: str) -> list[str]:
    """Separa a query em comandos por ';', ignorando ';' dentro de strings e comentários."""
    statements, buf = [], []
    i, n = 0, len(sql)
    in_single = in_double = in_backtick = False
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if not (in_single or in_double or in_backtick):
            if ch == "-" and nxt == "-":           # comentário de linha
                end = sql.find("\n", i)
                i = n if end == -1 else end
                continue
            if ch == "/" and nxt == "*":           # comentário de bloco
                end = sql.find("*/", i + 2)
                i = n if end == -1 else end + 2
                continue
            if ch == ";":
                stmt = "".join(buf).strip()
                if stmt:
                    statements.append(stmt)
                buf = []
                i += 1
                continue
        if ch == "'" and not (in_double or in_backtick):
            in_single = not in_single
        elif ch == '"' and not (in_single or in_backtick):
            in_double = not in_double
        elif ch == "`" and not (in_single or in_double):
            in_backtick = not in_backtick
        buf.append(ch)
        i += 1
    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)
    return statements


_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_orchestration(orch: Orchestration) -> list[str]:
    errors = []
    seen = set()
    for inp in orch.inputs:
        if not inp.is_valid():
            continue
        if not _ALIAS_RE.match(inp.alias.strip()):
            errors.append(f"Alias inválido '{inp.alias}': use letras, números e _ (sem começar com número).")
        if inp.alias.lower() in seen:
            errors.append(f"Alias duplicado: '{inp.alias}'.")
        seen.add(inp.alias.lower())
        if inp.kind == "s3" and not inp.source.strip().startswith(("s3://", "s3a://", "file:/", "/")):
            errors.append(f"Entrada '{inp.alias}': caminho deve começar com s3:// (ou ser um caminho local).")
        if inp.kind == "catalog" and "." not in inp.source and not orch.default_database:
            errors.append(f"Entrada '{inp.alias}': use 'database.tabela' ou defina um database padrão.")
    if not (1 <= int(orch.limit) <= 10000):
        errors.append("Limite de linhas deve estar entre 1 e 10000.")
    return errors


def build_spark_code(sql: str, orch: Orchestration, local_mode: bool = False) -> str:
    """Monta o script PySpark que roda as entradas + query e imprime o resultado."""
    statements = split_statements(sql)
    if not statements:
        raise ValueError("A query está vazia.")

    payload = {
        "statements": statements,
        "inputs": [asdict(i) for i in orch.inputs if i.is_valid()],
        "conf": {k: v for k, v in orch.spark_conf.items() if k and str(k).strip()},
        "db": orch.default_database.strip(),
        "limit": int(orch.limit),
        "marker": RESULT_MARKER,
    }

    # No Glue Interactive Sessions a variável `spark` já existe. Localmente também
    # (o motor local injeta `spark` no namespace).
    return f'''
import base64 as _b64, json as _json, time as _time, traceback as _tb
_p = _json.loads(_b64.b64decode("{_b64(payload)}").decode("utf-8"))
_t0 = _time.time()
_log = []

def _emit(obj):
    print(_p["marker"] + _b64.b64encode(_json.dumps(obj, default=str).encode("utf-8")).decode("ascii"))

try:
    for _k, _v in _p["conf"].items():
        try:
            spark.conf.set(_k, _v)
            _log.append("conf " + _k + "=" + str(_v))
        except Exception as _e:
            _log.append("AVISO: conf " + _k + " não aplicada (" + str(_e).splitlines()[0] + ")")

    if _p["db"]:
        spark.sql("USE " + _p["db"])
        _log.append("USE " + _p["db"])

    for _inp in _p["inputs"]:
        _alias, _src = _inp["alias"].strip(), _inp["source"].strip()
        if _inp["kind"] == "catalog":
            _df = spark.table(_src)
        else:
            _fmt = (_inp.get("fmt") or "parquet").lower()
            _r = spark.read.format(_fmt)
            for _opt in (_inp.get("options") or "").split(";"):
                if "=" in _opt:
                    _ok, _ov = _opt.split("=", 1)
                    _r = _r.option(_ok.strip(), _ov.strip())
            _df = _r.load(_src)
        if (_inp.get("filter") or "").strip():
            _df = _df.where(_inp["filter"])
        _df.createOrReplaceTempView(_alias)
        if _inp.get("cache"):
            spark.catalog.cacheTable(_alias)
        _log.append("view " + _alias + " <- " + _src)

    _df = None
    for _i, _s in enumerate(_p["statements"]):
        _df = spark.sql(_s)
        if _i < len(_p["statements"]) - 1:
            _df.collect()  # executa comandos intermediários (CREATE/INSERT/SET...)

    _cols = [{{"name": _f.name, "type": _f.dataType.simpleString()}} for _f in _df.schema.fields]
    _rows = []
    for _row in _df.limit(_p["limit"]).collect():
        _rows.append([None if _x is None else (_x if isinstance(_x, (int, float, bool, str)) else str(_x)) for _x in _row])
    _emit({{"ok": True, "columns": _cols, "rows": _rows, "elapsed": round(_time.time() - _t0, 2), "log": _log}})
except Exception as _e:
    _emit({{"ok": False, "error": type(_e).__name__ + ": " + str(_e), "traceback": _tb.format_exc(), "elapsed": round(_time.time() - _t0, 2), "log": _log}})
'''.strip()


def parse_result(output_text: str) -> dict | None:
    """Encontra a linha marcada no stdout e decodifica o JSON."""
    if not output_text:
        return None
    for line in reversed(output_text.splitlines()):
        idx = line.find(RESULT_MARKER)
        if idx != -1:
            raw = line[idx + len(RESULT_MARKER):].strip()
            return json.loads(base64.b64decode(raw).decode("utf-8"))
    return None

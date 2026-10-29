"""Leitura e gravação da orquestração em JSON (para editar à mão, salvar na máquina e reabrir)."""
from __future__ import annotations

import json
from dataclasses import asdict, fields

from code_builder import DEFAULT_LIMIT, InputSource, Orchestration

FORMAT_VERSION = 1
_INPUT_FIELDS = {f.name for f in fields(InputSource)}
_KINDS = {"catalog", "s3"}
_FORMATS = {"parquet", "csv", "json", "orc", "delta", "iceberg"}


class OrchestrationFormatError(ValueError):
    pass


def to_dict(orch: Orchestration) -> dict:
    return {
        "versao": FORMAT_VERSION,
        "database_padrao": orch.default_database,
        "limite_linhas": int(orch.limit),
        "spark_conf": dict(orch.spark_conf),
        "entradas": [asdict(i) for i in orch.inputs if i.is_valid()],
    }


def to_json(orch: Orchestration) -> str:
    return json.dumps(to_dict(orch), indent=2, ensure_ascii=False)


def _as_bool(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "sim", "s", "yes", "y")
    return bool(v)


def from_dict(data: dict) -> Orchestration:
    """Aceita o formato atual e o antigo (inputs / conf como lista / default_db / limit)."""
    if not isinstance(data, dict):
        raise OrchestrationFormatError("O arquivo deve conter um objeto JSON ({ ... }).")

    raw_inputs = data.get("entradas", data.get("inputs", []))
    if not isinstance(raw_inputs, list):
        raise OrchestrationFormatError("'entradas' deve ser uma lista.")

    inputs = []
    for n, item in enumerate(raw_inputs, start=1):
        if not isinstance(item, dict):
            raise OrchestrationFormatError(f"Entrada {n}: deve ser um objeto com alias, source...")
        values = {k: v for k, v in item.items() if k in _INPUT_FIELDS and v is not None}
        values = {k: (_as_bool(v) if k == "cache" else str(v).strip() if k in ("kind", "fmt") else str(v))
                  for k, v in values.items()}
        values["kind"] = values.get("kind") or "catalog"        # vazio = padrão
        values["fmt"] = (values.get("fmt") or "parquet").lower()
        inp = InputSource(**{"alias": "", **values})
        if inp.kind not in _KINDS:
            raise OrchestrationFormatError(f"Entrada {n} ({inp.alias}): tipo '{inp.kind}' inválido; use catalog ou s3.")
        if inp.fmt not in _FORMATS:
            raise OrchestrationFormatError(f"Entrada {n} ({inp.alias}): formato '{inp.fmt}' inválido.")
        if inp.is_valid():
            inputs.append(inp)

    conf = data.get("spark_conf", data.get("conf", {}))
    if isinstance(conf, list):                      # formato antigo: [{"chave": ..., "valor": ...}]
        conf = {str(c.get("chave", "")).strip(): str(c.get("valor", "")) for c in conf if isinstance(c, dict)}
    if not isinstance(conf, dict):
        raise OrchestrationFormatError("'spark_conf' deve ser um objeto {\"chave\": \"valor\"}.")
    conf = {str(k).strip(): str(v) for k, v in conf.items() if str(k).strip()}

    try:
        limit = int(data.get("limite_linhas", data.get("limit", DEFAULT_LIMIT)))
    except (TypeError, ValueError):
        raise OrchestrationFormatError("'limite_linhas' deve ser um número.") from None

    return Orchestration(inputs=inputs, spark_conf=conf,
                         default_database=str(data.get("database_padrao", data.get("default_db", "")) or ""),
                         limit=limit)


def from_json(text: str) -> Orchestration:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise OrchestrationFormatError(f"JSON inválido na linha {e.lineno}, coluna {e.colno}: {e.msg}") from None
    return from_dict(data)

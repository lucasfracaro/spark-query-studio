"""Estado da tela (de cada aba do navegador) e conversões da orquestração."""
from __future__ import annotations

import math

import pandas as pd
import streamlit as st

import settings_store
from code_builder import DEFAULT_LIMIT, InputSource, Orchestration

INPUT_COLUMNS = ["alias", "kind", "source", "fmt", "options", "filter", "cache"]


def init():
    ss = st.session_state
    if "account" not in ss:
        # a configuração vem do arquivo (configuracao.toml); problemas nele aparecem na tela
        lc = settings_store.load()
        ss.config_loaded, ss.account, ss.instance = lc, lc.account, lc.instance
    ss.setdefault("engine_kind", "glue")
    ss.setdefault("sql", "SELECT *\nFROM minha_view\nLIMIT 100")
    ss.setdefault("editor_version", 0)
    ss.setdefault("last_upload", None)
    ss.setdefault("result", None)
    ss.setdefault("inputs_base", pd.DataFrame([{
        "alias": "minha_view", "kind": "catalog", "source": "meu_database.minha_tabela",
        "fmt": "parquet", "options": "", "filter": "", "cache": False,
    }]))
    ss.setdefault("conf_base", pd.DataFrame([
        {"chave": "spark.sql.shuffle.partitions", "valor": "200"},
        {"chave": "spark.sql.adaptive.enabled", "valor": "true"},
    ]))
    ss.setdefault("orch_version", 0)   # muda a key dos editores ao importar
    ss.inputs_df = ss.inputs_base
    ss.conf_df = ss.conf_base
    ss.setdefault("default_db", "")
    ss.setdefault("row_limit", DEFAULT_LIMIT)
    ss.setdefault("check_refs", True)
    ss.setdefault("profile", False)
    ss.setdefault("result_version", 0)
    ss.setdefault("orch_draft", "")
    ss.setdefault("orch_draft_version", 0)
    ss.setdefault("open_orch_after", False)
    ss.setdefault("athena_result", None)
    ss.setdefault("sizing_data", None)       # o que a estimativa do tamanho da sessão coletou (importação)
    ss.setdefault("sizing", None)            # a última estimativa (aparece na barra lateral)
    ss.setdefault("allow_writes", False)     # modo somente leitura ligado por padrão
    ss.setdefault("engine_error", None)
    ss.setdefault("auto_start_done", False)  # "iniciar ao abrir" já foi feito nesta aba
    ss.setdefault("config_text_version", 0)


def clean(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v).strip()


def current_orchestration() -> Orchestration:
    ss = st.session_state
    inputs = []
    for _, r in ss.inputs_df.iterrows():
        inputs.append(InputSource(
            alias=clean(r.get("alias")), kind=clean(r.get("kind")) or "catalog",
            source=clean(r.get("source")), fmt=clean(r.get("fmt")) or "parquet",
            options=clean(r.get("options")), filter=clean(r.get("filter")),
            cache=bool(r.get("cache")) if not pd.isna(r.get("cache")) else False,
        ))
    conf = {clean(r["chave"]): clean(r["valor"]) for _, r in ss.conf_df.iterrows() if clean(r.get("chave"))}
    return Orchestration(inputs=inputs, spark_conf=conf, default_database=ss.default_db,
                         limit=int(ss.row_limit))


def apply_orchestration(orch: Orchestration, keep_conf: bool = False):
    """Substitui a orquestração da tela (os editores são recriados com os novos valores)."""
    ss = st.session_state
    ss.inputs_base = pd.DataFrame([{c: getattr(i, c) for c in INPUT_COLUMNS} for i in orch.inputs],
                                  columns=INPUT_COLUMNS)
    if keep_conf:
        # os editores são recriados: as configurações editadas na tela (e ainda não aplicadas) continuam
        ss.conf_base = ss.conf_df.copy()
    else:
        ss.conf_base = pd.DataFrame([{"chave": k, "valor": v} for k, v in orch.spark_conf.items()],
                                    columns=["chave", "valor"])
        ss.row_limit = int(orch.limit)
    ss.default_db = orch.default_database
    ss.orch_version += 1


def apply_config(lc: settings_store.LoadedConfig | None = None, account=None, instance=None):
    """Passa a usar a configuração indicada (lida do arquivo ou ajustada na tela)."""
    ss = st.session_state
    if lc is not None:
        ss.config_loaded = lc
        account = account if account is not None else lc.account
        instance = instance if instance is not None else lc.instance
    if account is not None:
        ss.account = account
    if instance is not None:
        ss.instance = instance
        ss.pop("args_base", None)          # o editor de argumentos extras é recriado

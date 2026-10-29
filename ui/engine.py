"""Ligação da tela com os motores Spark e com a sessão do app (session_hub)."""
from __future__ import annotations

import streamlit as st

import engines  # usado como engines.X: os testes trocam os motores por falsos


@st.cache_resource(show_spinner=False)
def _aws_client(service: str, region: str, auth_mode: str, profile: str, access_key: str, secret_key: str,
                session_token: str):
    """Cliente boto3 reaproveitado entre as interações (criar um custa caro e acontecia a cada tecla)."""
    conta = engines.AwsAccount(region=region, auth_mode=auth_mode, profile=profile, access_key=access_key,
                               secret_key=secret_key, session_token=session_token)
    return conta.client(service)


def make_engine():
    """Cria o motor. Se a configuração AWS estiver inválida (perfil inexistente, região vazia...),
    devolve None e guarda o erro em engine_error, em vez de derrubar a tela."""
    ss = st.session_state
    ss.engine_error = None
    if ss.engine_kind == "local":
        return engines.LocalSparkEngine(ss.instance, ss.account)
    acc = ss.account
    chave = (acc.region, acc.auth_mode, acc.profile, acc.access_key, acc.secret_key, acc.session_token)
    try:
        return engines.GlueSessionEngine(acc, ss.instance, glue_client=_aws_client("glue", *chave),
                                         sts_client=_aws_client("sts", *chave))
    except Exception as e:
        ss.engine_error = f"{type(e).__name__}: {e}"
        return None


def reader() -> str:
    """Como as entradas do catálogo são lidas: pelo GlueContext no Glue 4.0 com Lake Formation."""
    ss = st.session_state
    if ss.engine_kind == "glue" and engines.lake_formation_mode(ss.instance) == "glue_context":
        return "glue_context"
    return "spark"


def fingerprint() -> str | None:
    """Configuração da sessão desejada (para reaproveitar uma sessão anterior equivalente)."""
    ss = st.session_state
    return ss.instance.fingerprint() if ss.engine_kind == "glue" else None

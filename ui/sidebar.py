"""Barra lateral: onde executar, configuração, instância e a sessão Spark."""
from __future__ import annotations

import time

import pandas as pd
import streamlit as st

import settings_store
from code_builder import build_clear_cache_code
from engines import (
    DEFAULT_GLUE_VERSION,
    DEFAULT_WORKER_TYPE,
    GLUE_VERSION_NOTES,
    GLUE_VERSIONS,
    GLUE_WORKER_TYPES,
    InstanceConfig,
)
from session_hub import HUB
from ui.config_dialog import file_status, lake_formation_help, open_config_dialog
from ui.engine import fingerprint, make_engine
from ui.sizing import sidebar_hint
from ui.state import apply_config, clean


def server_is_local_only() -> bool:
    endereco = (st.get_option("server.address") or "").strip().lower()
    return endereco in ("localhost", "127.0.0.1", "::1") or endereco.startswith("unix://")


def render():
    """Desenha a barra lateral e devolve o motor (None se a conexão com a AWS não pôde ser preparada)."""
    ss = st.session_state
    with st.sidebar:
        st.markdown("### ⚡ Motor Spark")
        ss.engine_kind = st.radio(
            "Onde executar", ["glue", "local"], horizontal=True,
            index=["glue", "local"].index(ss.engine_kind),
            format_func={"glue": "AWS Glue", "local": "Spark local"}.get, label_visibility="collapsed")

        if st.button("⚙️ Configuração", width="stretch",
                     help="Conta AWS, role e sessão. Vêm do arquivo configuracao.toml; aqui dá para conferir, "
                          "corrigir e salvar."):
            open_config_dialog()
        st.caption(file_status(ss.config_loaded))

        if ss.engine_kind == "glue":
            acc = ss.account
            sem_chaves = acc.auth_mode == "keys" and not (acc.access_key and acc.secret_key)
            st.caption(f"Região **{acc.region}** · auth **{acc.auth_mode}**"
                       + ("" if acc.glue_role_arn else " · ⚠️ role não definida")
                       + (" · ⚠️ informe as chaves em ⚙️ Configuração" if sem_chaves else ""))
            _instancia()

        if not server_is_local_only():
            st.warning("⚠️ O app está aceitando conexões de outras máquinas da rede. Abra pelo `run.sh` / "
                       "`run_windows.bat` ou use `streamlit run app.py --server.address localhost`.")

        st.markdown("### 🔌 Sessão")
        engine = make_engine()
        if engine is None:
            st.error(f"Não consegui preparar a conexão com a AWS ({ss.engine_error}). "
                     "Revise em **⚙️ Configuração**.")
            return None
        _iniciar_ao_abrir(engine)
        slot = HUB.slot(engine)
        HUB.find_reusable(engine)            # procura sessões anteriores suas, em segundo plano (uma vez)
        # enquanto a sessão sobe (ou a busca roda), o painel se atualiza sozinho
        atualizar = 2 if (slot.starting or slot.searching) else None
        st.fragment(_painel_sessao, run_every=atualizar)(engine)
    return engine


def _instancia():
    ss = st.session_state
    st.markdown("### 🖥️ Instância")
    inst: InstanceConfig = ss.instance
    tipos = list(GLUE_WORKER_TYPES)
    inst.worker_type = st.selectbox(
        "Tipo de worker", tipos,
        index=tipos.index(inst.worker_type if inst.worker_type in GLUE_WORKER_TYPES else DEFAULT_WORKER_TYPE),
        format_func=lambda w: f"{w} — {GLUE_WORKER_TYPES[w].descricao()}")
    inst.num_workers = st.slider("Número de workers", settings_store.MIN_WORKERS, settings_store.MAX_WORKERS,
                                 int(inst.num_workers))
    versoes = list(GLUE_VERSIONS)
    inst.glue_version = st.selectbox(
        "Versão do Glue", versoes,
        index=versoes.index(inst.glue_version if inst.glue_version in GLUE_VERSIONS else DEFAULT_GLUE_VERSION),
        format_func=lambda v: f"{v} — {GLUE_VERSIONS[v]}",
        help="Use a mesma versão do job em produção: o Spark muda de uma versão para outra.")
    if inst.glue_version in GLUE_VERSION_NOTES:
        st.caption("⚠️ " + GLUE_VERSION_NOTES[inst.glue_version])
    inst.idle_timeout_min = st.number_input("Encerrar sozinha após (min sem uso)", settings_store.MIN_IDLE,
                                            settings_store.MAX_IDLE, int(inst.idle_timeout_min))
    inst.lake_formation = st.checkbox("Ler as tabelas pelo Lake Formation", inst.lake_formation,
                                      help=lake_formation_help(inst.glue_version))
    dpu = GLUE_WORKER_TYPES[inst.worker_type].dpu * inst.num_workers
    st.caption(f"≈ **{dpu:g} DPU** enquanto a sessão estiver ativa."
               + (" Com 2 workers, só 1 processa os dados (o outro coordena): para tabelas grandes, use mais."
                  if inst.num_workers <= 2 else ""))
    sidebar_hint(inst)
    with st.expander("Argumentos extras do job"):
        st.caption("Ex: `--datalake-formats` = `iceberg`, `--additional-python-modules` = `pyarrow==14`")
        if "args_base" not in ss:
            ss.args_base = pd.DataFrame([{"argumento": k, "valor": v} for k, v in inst.extra_args.items()]
                                        or [{"argumento": "", "valor": ""}])
        args_df = st.data_editor(ss.args_base, num_rows="dynamic", width="stretch", key="args_editor")
        inst.extra_args = {clean(r["argumento"]): clean(r["valor"]) for _, r in args_df.iterrows()
                           if clean(r.get("argumento"))}
    if st.button("💾 Salvar no arquivo de configuração", width="stretch"):
        lc = ss.config_loaded
        try:
            caminho = settings_store.save(ss.account, inst, include_secrets=lc.has_secrets)
        except OSError as e:
            st.error(f"Não consegui gravar o arquivo: {e}")
        else:
            apply_config(settings_store.load(), account=ss.account)
            ss.config_text_version += 1
            st.toast(f"Configuração salva em {caminho}", icon="✅")


def _iniciar_ao_abrir(engine):
    """Opção "iniciar a sessão assim que o app abrir": uma vez por aba, em segundo plano."""
    ss = st.session_state
    if ss.auto_start_done or ss.engine_kind != "glue" or not ss.instance.start_on_open:
        return
    ss.auto_start_done = True
    slot = HUB.slot(engine)
    if not slot.session_id and not slot.starting:
        HUB.start_background(engine, fingerprint())


def _so_o_painel_esta_rodando() -> bool:
    """True quando o Streamlit está redesenhando só o painel (atualização automática), e não a página."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        ctx = get_script_run_ctx()
        return bool(ctx is not None and getattr(ctx, "fragment_ids_this_run", None))
    except Exception:
        return False


def _painel_sessao(engine):
    ss = st.session_state
    slot = HUB.slot(engine)
    ocupado = slot.starting or slot.searching
    if ss.get("painel_ocupado") and not ocupado and _so_o_painel_esta_rodando():
        # terminou enquanto só o painel se atualizava: redesenha a página toda (cabeçalho, botões).
        # Numa execução da página inteira isso não é preciso (e perderia o clique que a disparou).
        ss.painel_ocupado = False
        st.rerun()
    ss.painel_ocupado = ocupado

    erro_status = None
    try:
        status = HUB.status(engine)
    except Exception as e:                   # falha momentânea: a sessão continua registrada
        status, erro_status = slot.status, e

    if slot.starting:
        st.info(f"⏳ {slot.progress or 'Iniciando a sessão…'}")
        st.caption("Pode continuar montando a query: a execução usa esta sessão assim que ela ficar pronta.")
    elif erro_status is not None and slot.session_id:
        st.warning(f"Não consegui consultar a sessão `{slot.session_id}` ({type(erro_status).__name__}). "
                   "Ela continua registrada; a próxima execução tenta de novo.")
        _botoes(engine, status)
    elif status == "READY":
        rotulo = slot.session_id if engine.kind == "glue" else "Spark local"
        st.success(f"Ativa: `{rotulo}`")
        if engine.kind == "glue":
            fim = slot.expected_end()
            if fim:
                st.caption(f"Encerra sozinha por volta das {time.strftime('%H:%M', time.localtime(fim))} "
                           "se ficar sem uso.")
            if slot.adopted and slot.adopted == slot.session_id:
                st.caption("♻️ Sessão criada antes, com a mesma configuração, reaproveitada.")
            if slot.fingerprint and slot.fingerprint != fingerprint():
                st.warning(f"A sessão ativa foi criada com **{slot.fingerprint}**. "
                           "Encerre para aplicar a nova configuração.")
        _botoes(engine, status)
    elif engine.kind == "glue" and slot.session_id:
        st.info(f"Sessão `{slot.session_id}` iniciando ({status}). Ela será usada na próxima execução.")
        _botoes(engine, status)
    else:
        if slot.error:
            st.error(f"A sessão não iniciou: {slot.error}")
        _oferta_reaproveitar(engine, slot)
        st.info("Nenhuma sessão ativa. Ela é criada na primeira execução (no Glue leva ~30–90 s) "
                "e depois reaproveitada pelas próximas.")
        if st.button("▶️ Iniciar sessão agora", width="stretch",
                     help="Sobe a sessão em segundo plano enquanto você monta a query."):
            HUB.start_background(engine, fingerprint())
            st.rerun()


def _botoes(engine, status):
    c1, c2 = st.columns(2)
    if c1.button("⏹️ Encerrar", width="stretch", key="encerrar_sessao"):
        try:
            HUB.stop(engine)
        except Exception as e:
            st.error(f"Não consegui encerrar a sessão: {e}")
        else:
            st.rerun()
    if c2.button("🧹 Limpar cache", width="stretch", disabled=status != "READY",
                 help="Tira da memória as entradas guardadas em cache, para reler dados que mudaram no S3."):
        try:
            res = HUB.run(engine, build_clear_cache_code(), fingerprint=fingerprint())
        except Exception as e:
            st.error(f"Não consegui limpar o cache: {e}")
        else:
            if res.get("ok"):
                st.toast((res.get("log") or ["Cache limpo"])[0].capitalize(), icon="🧹")
            else:
                st.error(f"Não consegui limpar o cache: {res.get('error')}")


def _oferta_reaproveitar(engine, slot):
    anteriores = [s for s in (slot.reusable or []) if s.get("Id") != slot.session_id]
    if engine.kind != "glue" or not anteriores:
        return
    s0 = anteriores[0]
    st.info(f"Você tem uma sessão ainda ativa: `{s0['Id']}` ({s0.get('Status')}, {s0.get('fingerprint')}). "
            "Reaproveitar evita esperar outra subir e pagar pelas duas.")
    r1, r2 = st.columns(2)
    if r1.button("♻️ Reaproveitar", width="stretch"):
        HUB.adopt(engine, s0)
        st.rerun()
    if r2.button("⏹️ Encerrar", width="stretch", key="encerrar_anterior"):
        try:
            HUB.stop(engine, s0["Id"])
        except Exception as e:
            st.error(f"Não consegui encerrar a sessão: {e}")
        else:
            st.rerun()

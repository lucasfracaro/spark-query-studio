"""Passo 2: a query (editor, conferências e código gerado) e a execução."""
from __future__ import annotations

import time

import streamlit as st

from code_builder import build_spark_code, split_statements, validate_orchestration
from engines import EngineError
from session_hub import HUB
from sql_refs import check_query_against_orchestration, find_write_statements
from ui import style
from ui.editor import ace
from ui.engine import fingerprint, reader
from ui.state import current_orchestration


def render(engine):
    ss = st.session_state
    style.step(2)
    st.subheader("Query")

    up_sql = st.file_uploader("Importar query de arquivo (.sql / .txt) — ou escreva direto no editor abaixo",
                              type=["sql", "txt", "hql"], key="sql_upload")
    if up_sql is not None and ss.last_upload != up_sql.file_id:
        ss.sql = up_sql.getvalue().decode("utf-8", errors="replace")
        ss.last_upload = up_sql.file_id
        ss.editor_version += 1          # força o editor a recarregar com o conteúdo importado
        st.rerun()

    st_ace = ace()
    if st_ace is not None:
        edited = st_ace(value=ss.sql, language="sql", theme="tomorrow_night", font_size=14, height=320,
                        show_gutter=True, wrap=True, auto_update=True, key=f"ace_{ss.editor_version}")
        if edited is not None:
            ss.sql = edited
    else:
        ss.sql = st.text_area("Editor", ss.sql, height=320, key=f"ta_{ss.editor_version}",
                              label_visibility="collapsed")

    leitor = reader()
    orch = current_orchestration()
    n_stmts = len(split_statements(ss.sql))
    st.caption(f"{n_stmts} comando(s). Separe vários com `;` — todos são executados e o resultado do **último** "
               "é exibido.")
    ref_check = check_query_against_orchestration(ss.sql, orch.inputs, require_alias=leitor == "glue_context")
    if ref_check.missing:
        st.caption("⚠️ Fora da orquestração: " + ", ".join(f"`{m.name}`" for m in ref_check.missing))
    if ref_check.full_names:
        st.caption("⚠️ Use o alias: " + ", ".join(f"`{f.name}` → `{f.alias}`" for f in ref_check.full_names))
    escritas = [] if ss.allow_writes else find_write_statements(ss.sql)
    if escritas:
        st.caption("🔒 Modo somente leitura: " + ", ".join(f"o comando {w.number} ({w.keyword})" for w in escritas)
                   + " grava dados e será bloqueado.")
    if leitor == "glue_context":
        st.caption("🔐 Lake Formation no Glue 4.0: as entradas `catalog` são lidas pelo GlueContext.")

    b1, b2, b3, _ = st.columns([1.2, 1, 1, 3])
    run_clicked = b1.button("▶️ Executar", type="primary", width="stretch")
    b2.download_button("💾 Salvar .sql", ss.sql, "query.sql", "text/plain", width="stretch")
    if b3.button("🧹 Limpar", width="stretch"):
        ss.sql, ss.result = "", None
        ss.editor_version += 1
        st.rerun()

    with st.expander("Ver código PySpark gerado"):
        try:
            st.caption("É exatamente o código que vai rodar. Dá para copiar e rodar num notebook do Glue.")
            st.code(build_spark_code(ss.sql, orch, profile=ss.profile, reader=leitor, count=ss.count_rows), language="python")
        except ValueError as e:
            st.caption(str(e))

    _cancelar(engine)
    if run_clicked:
        _executar(engine, orch, ref_check, escritas, leitor)


def _cancelar(engine):
    """O clique em Cancelar reinicia o script (interrompendo a espera da execução em andamento);
    aqui a query que estava rodando no Glue é cancelada de fato."""
    ss = st.session_state
    if not (ss.get("cancelar_execucao") and engine is not None and ss.engine_kind == "glue"):
        return
    try:
        if HUB.cancel_running(engine):
            ss.result = {"ok": False, "error": "Execução cancelada.", "traceback": "", "log": []}
    except Exception as e:
        st.error(f"Não consegui cancelar a execução: {e}")


def _executar(engine, orch, ref_check, escritas, leitor: str):
    ss = st.session_state
    errors = validate_orchestration(orch)
    if not ss.sql.strip():
        errors.append("A query está vazia.")
    if ss.check_refs and not errors:
        errors += [m.message() for m in ref_check.missing] + [f.message() for f in ref_check.full_names]
    if not errors:
        errors += [w.message() for w in escritas]
    code = None
    if not errors:
        try:
            code = build_spark_code(ss.sql, orch, profile=ss.profile, reader=leitor, count=ss.count_rows)
        except ValueError as e:
            errors.append(str(e))
    if engine is None and not errors:
        errors.append(f"A conexão com a AWS não está pronta ({ss.engine_error}). Revise em **⚙️ Configuração**.")
    for aviso in ref_check.warnings:
        st.warning(aviso)
    if errors:
        for e in errors:
            st.error(e)
        return

    ss.result_version += 1
    t0 = time.time()
    botao_cancelar = st.empty()
    if ss.engine_kind == "glue":
        botao_cancelar.button("⏹️ Cancelar execução", key="cancelar_execucao")
    with st.status("Preparando…", expanded=False) as box:
        try:
            res = HUB.run(engine, code, lambda m: box.update(label=m), fingerprint=fingerprint())
            res["total"] = round(time.time() - t0, 1)
            # o que gerou este resultado: o "baixar todas as linhas" roda exatamente a mesma coisa
            res["origem"] = {"sql": ss.sql, "orch": orch, "reader": leitor}
            ss.result = res
            box.update(label=("Concluído" if res.get("ok") else "Erro na query") + f" em {res['total']}s",
                       state="complete" if res.get("ok") else "error")
        except EngineError as e:
            box.update(label="Falha", state="error")
            ss.result = {"ok": False, "error": str(e), "traceback": "", "log": []}
        except Exception as e:
            box.update(label="Falha", state="error")
            ss.result = {"ok": False, "error": f"{type(e).__name__}: {e}", "traceback": "", "log": []}
    botao_cancelar.empty()

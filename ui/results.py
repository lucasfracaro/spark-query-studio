"""Passo 3: o resultado (dados, exportação em CSV, metadados) ou o erro, resumido."""
from __future__ import annotations

import time

import pandas as pd
import streamlit as st

import metadata
from export import FORMATS, to_csv_bytes
from spark_errors import summarize
from sql_refs import missing_from_spark_error
from ui import style


def unique_names(cols: list[str]) -> list[str]:
    """Nomes de coluna duplicados (ex: JOIN sem alias) quebram o DataFrame: x, x -> x, x_2."""
    seen, uniq = {}, []
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
        uniq.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return uniq


def render():
    ss = st.session_state
    res = ss.result
    if not res:
        return
    style.step(3)
    st.subheader("Resultado")
    if res.get("ok"):
        _dados(res)
    else:
        _erro(res)
    if res.get("log"):
        with st.expander("Log de orquestração"):
            st.code("\n".join(res["log"]), language="text")


def _dados(res: dict):
    uniq = unique_names([c["name"] for c in res["columns"]])
    df = pd.DataFrame(res["rows"], columns=uniq)

    m1, m2, m3 = st.columns(3)
    m1.metric("Linhas exibidas", f"{len(df):,}".replace(",", "."))
    m2.metric("Colunas", len(df.columns))
    m3.metric("Tempo", f"{res.get('total', res.get('elapsed', 0))} s")

    t_dados, t_meta = st.tabs(["📋 Dados", "🧾 Metadados"])
    with t_dados:
        st.dataframe(df, width="stretch", height=min(38 + 35 * len(df), 560), hide_index=True)
        c1, c2, _ = st.columns([1.4, 1, 1.6])
        formato = c1.selectbox("Formato do CSV", list(FORMATS), format_func=FORMATS.get,
                               help="Excel em português espera ; como separador e vírgula nos decimais.")
        tipos = [c.get("type", "") for c in res["columns"]]
        c2.markdown("<div style='height:1.75rem'></div>", unsafe_allow_html=True)
        c2.download_button("⬇️ Exportar CSV", to_csv_bytes(df, tipos, formato),
                           f"resultado_{time.strftime('%Y%m%d_%H%M%S')}.csv", "text/csv", type="primary",
                           width="stretch")
    with t_meta:
        render_metadata(res, uniq)


def _erro(res: dict):
    erro = res.get("error", "") or "Erro desconhecido"
    faltando = missing_from_spark_error(erro)
    if faltando:
        tipo, nome = faltando
        if tipo == "tabela":
            st.error(f"A base `{nome}` não foi encontrada pelo Spark. Ela precisa estar na orquestração: "
                     f"adicione-a em Passo 1 › Entradas de dados.")
        else:
            st.error(f"O banco `{nome}` não foi encontrado pelo Spark. Confira o nome na orquestração "
                     f"(Passo 1 › Entradas de dados) e o database padrão.")
    else:
        resumo = summarize(erro, res.get("traceback", ""))
        st.error(f"**{resumo.title}**")
        if resumo.hint:
            st.info(resumo.hint)
        if resumo.detail and resumo.detail.strip() != resumo.title:
            st.code(resumo.detail, language="text")
    if res.get("traceback") or len(erro) > 300 or faltando:
        with st.expander("Mensagem completa do Spark"):
            st.code(erro + ("\n\n" + res["traceback"] if res.get("traceback") else ""), language="text")


def render_metadata(res: dict, col_names: list[str]):
    ss = st.session_state
    cols = [dict(c, name=n) for c, n in zip(res["columns"], col_names, strict=True)]
    profile = res.get("profile")
    rows = metadata.build_dictionary(cols, profile)
    if profile:
        st.caption(f"Estatísticas calculadas sobre o resultado completo: **{profile['rows']:,}** linhas."
                   .replace(",", "."))
    else:
        st.caption("Para ver nulos, distintos, mínimo e máximo, ligue **Calcular estatísticas** em Passo 1 › Opções.")
    editado = st.data_editor(
        pd.DataFrame(rows), hide_index=True, width="stretch", key=f"meta_{ss.result_version}",
        disabled=[c for c in rows[0].keys() if c != "descricao"] if rows else True,
        column_config={"descricao": st.column_config.TextColumn("descrição", width="large",
                                                                help="Preencha: entra no dicionário e no DDL")})
    rows = editado.to_dict("records")

    c1, c2 = st.columns(2)
    carimbo = time.strftime("%Y%m%d_%H%M%S")
    c1.download_button("⬇️ Dicionário de dados (CSV)",
                       pd.DataFrame(rows).to_csv(index=False, sep=";").encode("utf-8-sig"),
                       f"metadados_{carimbo}.csv", "text/csv", width="stretch", on_click="ignore")
    c2.download_button("⬇️ Dicionário de dados (JSON)",
                       metadata.dictionary_to_json(rows, rows_total=(profile or {}).get("rows")),
                       f"metadados_{carimbo}.json", "application/json", width="stretch", on_click="ignore")

    st.markdown("##### DDL da tabela no Athena")
    d1, d2, d3 = st.columns(3)
    db = d1.text_input("Database", ss.default_db, key="ddl_db")
    tabela = d2.text_input("Tabela", "", key="ddl_tabela", placeholder="nome_da_tabela")
    fmt = d3.selectbox("Formato", ["parquet", "orc", "csv", "json"], key="ddl_fmt")
    local = st.text_input("Localização no S3", "", key="ddl_local", placeholder="s3://bucket/caminho/tabela/")
    parts = st.multiselect("Colunas de partição", [r["coluna"] for r in rows], key="ddl_parts")
    comentario = st.text_input("Descrição da tabela (opcional)", "", key="ddl_coment")
    if tabela.strip() and local.strip():
        try:
            ddl = metadata.build_ddl(rows, db, tabela, local, parts, fmt, comentario)
            st.code(ddl, language="sql")
            st.download_button("⬇️ Salvar DDL", ddl, f"{tabela.strip()}.sql", "text/plain", on_click="ignore")
        except ValueError as e:
            st.error(str(e))
    else:
        st.caption("Informe a tabela e a localização no S3 para gerar o DDL.")

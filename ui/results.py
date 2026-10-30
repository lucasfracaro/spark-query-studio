"""Passo 3: o resultado (dados, exportação em CSV, metadados) ou o erro, resumido."""
from __future__ import annotations

import time

import pandas as pd
import streamlit as st

import metadata
from code_builder import EXPORT_MAX_ROWS
from export import FORMATS, fetch_full_result, rows_to_csv_bytes
from session_hub import HUB
from spark_errors import summarize
from sql_refs import missing_from_spark_error
from ui import style
from ui.engine import fingerprint


def unique_names(cols: list[str]) -> list[str]:
    """Nomes de coluna duplicados (ex: JOIN sem alias) quebram o DataFrame: x, x -> x, x_2."""
    seen, uniq = {}, []
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
        uniq.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return uniq


def render(engine=None):
    ss = st.session_state
    res = ss.result
    if not res:
        return
    style.step(3)
    st.subheader("Resultado")
    for linha in res.get("log") or []:
        if linha.startswith("AVISO:"):          # ex.: database padrão que não existe, configuração não aplicada
            st.caption("⚠️ " + linha[len("AVISO:"):].strip())
    if res.get("ok"):
        _dados(res, engine)
    else:
        _erro(res)
    if res.get("log"):
        with st.expander("Log de orquestração"):
            st.code("\n".join(res["log"]), language="text")


def _n(numero: int) -> str:
    return f"{numero:,}".replace(",", ".")


def _dados(res: dict, engine=None):
    uniq = unique_names([c["name"] for c in res["columns"]])
    df = pd.DataFrame(res["rows"], columns=uniq)
    total = res.get("total_rows")

    m1, m2, m3 = st.columns(3)
    if total is None:
        m1.metric("Linhas exibidas", _n(len(df)))
    else:
        m1.metric("Linhas no resultado", _n(total))
    m2.metric("Colunas", len(df.columns))
    m3.metric("Tempo", f"{res.get('total', res.get('elapsed', 0))} s")

    t_dados, t_meta = st.tabs(["📋 Dados", "🧾 Metadados"])
    with t_dados:
        if total is not None and total > len(df):
            st.caption(f"Na tela, as primeiras {_n(len(df))} de {_n(total)} linhas (**Linhas exibidas**, em ⚙️ "
                       "Opções). O CSV leva todas.")
        st.dataframe(df, width="stretch", height=min(38 + 35 * len(df), 560), hide_index=True)
        c1, c2 = st.columns([1.4, 1.6], vertical_alignment="bottom")
        formato = c1.selectbox("Formato do CSV", list(FORMATS), format_func=FORMATS.get,
                               help="Excel em português espera ; como separador e vírgula nos decimais.")
        _botao_csv(c2, res, uniq, formato, engine)
    with t_meta:
        render_metadata(res, uniq)


def _completo_na_tela(res: dict) -> bool:
    """As linhas da tela já são o resultado inteiro?"""
    total, na_tela = res.get("total_rows"), len(res["rows"])
    if total is not None:
        return total <= na_tela
    origem = res.get("origem")
    return bool(origem) and na_tela < int(origem["orch"].limit)      # nem chegou no limite da tela


def _botao_csv(onde, res: dict, nomes: list[str], formato: str, engine):
    """Exportar CSV, sempre com todas as linhas. Se elas já estão na tela, o arquivo sai delas na hora; senão, ele
    é gerado na sessão quando o botão é clicado (do resultado que ficou guardado, sem rodar a query de novo)."""
    tipos = [c.get("type", "") for c in res["columns"]]
    total, origem = res.get("total_rows"), res.get("origem")
    nome = f"resultado_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    if _completo_na_tela(res) or engine is None or not origem:
        rotulo = (f"⬇️ Exportar CSV ({_n(len(res['rows']))} linhas)" if _completo_na_tela(res)
                  else "⬇️ Exportar CSV (linhas exibidas)")
        onde.download_button(rotulo, rows_to_csv_bytes(nomes, res["rows"], tipos, formato), nome, "text/csv",
                             type="primary", width="stretch")
        return
    if total is None:
        rotulo = "⬇️ Exportar CSV (todas as linhas)"
    elif total > EXPORT_MAX_ROWS:
        rotulo = f"⬇️ Exportar CSV (primeiras {_n(EXPORT_MAX_ROWS)} de {_n(total)} linhas)"
    else:
        rotulo = f"⬇️ Exportar CSV ({_n(total)} linhas)"
    onde.download_button(
        rotulo, csv_generator(engine, origem, res.get("result_key"), formato, fingerprint()), nome, "text/csv",
        type="primary", width="stretch", on_click="ignore",
        help="O arquivo é gerado quando você clica, com todas as linhas: ele sai do resultado que ficou na sessão "
             "(ou a query roda de novo, se a sessão foi recriada) e vem em partes pela própria sessão.")
    if total is not None and total > EXPORT_MAX_ROWS:
        st.caption(f"O Excel abre no máximo {_n(EXPORT_MAX_ROWS + 1)} linhas (com o cabeçalho): o CSV leva as "
                   "primeiras. Para ter tudo, divida a query (por data, por exemplo) e exporte cada parte.")


def csv_generator(engine, origem: dict, result_key: str | None, formato: str, fp: str | None):
    """A função que o botão chama no clique. O Streamlit a roda numa thread separada, sem acesso à tela: tudo o
    que ela usa vem daqui."""
    def gerar() -> bytes:
        dados, _info = fetch_full_result(
            lambda codigo, on_status: HUB.run(engine, codigo, on_status, fingerprint=fp),
            origem["sql"], origem["orch"], origem["reader"], formato, result_key=result_key)
        return dados
    return gerar


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

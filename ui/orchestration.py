"""Passo 1: orquestração das entradas, com as janelas de orquestração (arquivo) e de importação do Athena."""
from __future__ import annotations

import pandas as pd
import streamlit as st

import athena_import
import orchestration_io
from code_builder import validate_orchestration
from ui import sizing as ui_sizing
from ui import style
from ui.editor import ace
from ui.state import apply_orchestration, current_orchestration


# ------------------------------------------------------------------------------------
# Janela: orquestração (editar como JSON, salvar na máquina, abrir arquivo)
# ------------------------------------------------------------------------------------
@st.dialog("Orquestração", width="large")
def orchestration_dialog():
    orchestration_body()


def orchestration_body():
    """Conteúdo da janela de orquestração (separado dela para poder ser testado sem abrir a janela)."""
    ss = st.session_state
    st.caption("Ajuste o que for preciso e clique em **Aplicar**. Use **Salvar na máquina** para guardar o "
               "arquivo e **Abrir arquivo** para carregar uma orquestração salva.")
    up = st.file_uploader("Abrir arquivo", type=["json"], key=f"orch_file_{ss.orch_draft_version}")
    if up is not None:
        ss.orch_draft = up.getvalue().decode("utf-8", errors="replace")
        ss.orch_draft_version += 1
        st.rerun(scope="fragment")

    key = f"orch_text_{ss.orch_draft_version}"
    st_ace = ace()
    if st_ace is not None:
        texto = st_ace(value=ss.orch_draft, language="json", theme="tomorrow_night", font_size=13, height=420,
                       show_gutter=True, wrap=True, auto_update=True, key=key)
        texto = ss.orch_draft if texto is None else texto
    else:
        texto = st.text_area("JSON da orquestração", ss.orch_draft, height=420, key=key)

    erro, orch = None, None
    try:
        orch = orchestration_io.from_json(texto)
    except orchestration_io.OrchestrationFormatError as e:
        erro = str(e)
    if erro:
        st.error(erro)
    else:
        for e in validate_orchestration(orch):
            st.warning(e)
        st.caption(f"{len(orch.inputs)} entrada(s) · {len(orch.spark_conf)} configuração(ões) Spark")

    c1, c2, c3 = st.columns([1, 1, 1])
    nome = c3.text_input("Nome do arquivo", "orquestracao.json", label_visibility="collapsed")
    c2.download_button("⬇️ Salvar na máquina", texto, nome if nome.endswith(".json") else nome + ".json",
                       "application/json", width="stretch", on_click="ignore")
    if c1.button("✅ Aplicar", type="primary", width="stretch", disabled=orch is None):
        apply_orchestration(orch)
        ss.orch_draft = orchestration_io.to_json(orch)
        st.rerun()


def open_orchestration_dialog():
    ss = st.session_state
    ss.orch_draft = orchestration_io.to_json(current_orchestration())
    ss.orch_draft_version += 1
    orchestration_dialog()


# ------------------------------------------------------------------------------------
# Janela: importar query do Athena
# ------------------------------------------------------------------------------------
@st.dialog("Importar query do Athena", width="large")
def athena_dialog():
    athena_body()


def athena_body():
    """Conteúdo da janela de importação do Athena (separado dela para poder ser testado)."""
    ss = st.session_state
    if not athena_import.sqlglot_available():
        st.error("Para importar queries do Athena instale o pacote **sqlglot** (`pip install sqlglot`).")
        return
    st.caption("Cole a query do Athena ou abra o arquivo. A orquestração é montada a partir das bases usadas e dos "
               "filtros de partição das CTEs, e a query é traduzida para Spark SQL.")
    up = st.file_uploader("Abrir arquivo", type=["sql", "txt"], key="athena_file")
    inicial = up.getvalue().decode("utf-8", errors="replace") if up is not None else ""
    sql_athena = st.text_area("Query do Athena", inicial, height=260, key=f"athena_sql_{up.file_id if up else 0}")
    c1, c2 = st.columns(2)
    db_padrao = c1.text_input("Database padrão", ss.default_db, placeholder="opcional",
                              help="Usado para tabelas escritas sem database na query e como USE na execução. Vem "
                                   "da configuração (⚙️) e pode ficar vazio.")
    usar_glue = c2.checkbox("Consultar o Glue para saber as colunas de partição", ss.engine_kind == "glue",
                            help="Sem o Glue, as colunas de partição são supostas pelo nome (dt, anomes...).")
    estimar = c2.checkbox("Estimar o tamanho da sessão (workers e tipo de worker)", ss.engine_kind == "glue",
                          help="Pelo tamanho das partições que os filtros escolhem (Glue Data Catalog; S3 só nos "
                               "buckets da sua conta) e pela execução desta query no histórico do Athena, se houver. "
                               "Nada roda no Athena.")
    if estimar:
        ui_sizing.options()

    if st.button("🔎 Gerar orquestração", type="primary", disabled=not sql_athena.strip()):
        lookup = None
        ss.sizing_data = None
        if usar_glue:
            try:
                lookup = athena_import.glue_partition_lookup(ss.account.client("glue"))
            except Exception as e:
                st.warning(f"Não consegui conectar ao Glue ({e}). As partições serão supostas pelo nome.")
        try:
            ss.athena_result = athena_import.import_athena_query(sql_athena, lookup, db_padrao.strip())
        except ValueError as e:
            ss.athena_result = None
            st.error(str(e))
        except Exception as e:      # qualquer outra falha vira mensagem, sem derrubar a janela
            ss.athena_result = None
            st.error(f"Não consegui importar a query ({type(e).__name__}: {e}).")
        if ss.athena_result is not None and estimar:
            with st.spinner("Estimando o tamanho da sessão (catálogo do Glue e histórico do Athena)…"):
                try:
                    ss.sizing_data = ui_sizing.collect(ss.athena_result, sql_athena)
                except Exception as e:
                    st.warning(f"Não consegui estimar o tamanho da sessão ({type(e).__name__}: {e}).")

    r = ss.athena_result
    if r is None:
        return
    st.divider()
    for n in r.notes:
        (st.warning if n.level == "atencao" else st.info)(n.text)
    st.dataframe(pd.DataFrame([{
        "alias": t.alias, "origem": f"{t.database}.{t.table}" if t.database else t.table,
        "partições": ", ".join(t.partition_keys) or "—",
        "partições vindas de": "Glue" if t.partition_source == "glue" else "suposição pelo nome",
        "filtro": t.filter or "—"} for t in r.tables]), hide_index=True, width="stretch")
    with st.expander("Query traduzida para Spark SQL"):
        st.code(r.spark_sql, language="sql")
    estimativa = None
    if ss.sizing_data is not None:
        st.divider()
        estimativa = ui_sizing.render(ss.sizing_data)
    if st.button("✅ Aplicar e revisar a orquestração", type="primary"):
        apply_orchestration(r.orchestration, keep_conf=True)
        ui_sizing.apply(estimativa)
        ss.sql = r.spark_sql
        ss.editor_version += 1
        ss.athena_result = None
        ss.sizing_data = None
        ss.open_orch_after = True
        st.rerun()


# ------------------------------------------------------------------------------------
# Passo 1
# ------------------------------------------------------------------------------------
def render():
    ss = st.session_state
    style.step(1)
    st.subheader("Orquestração das entradas")
    b_ath, b_orch, _ = st.columns([1, 1, 2])
    # As janelas só abrem depois das abas abaixo: assim elas já veem as edições feitas nas tabelas
    # (as tabelas só são lidas quando são desenhadas).
    abrir_janela = None
    if b_ath.button("📥 Importar query do Athena", width="stretch"):
        abrir_janela = "athena"
    if b_orch.button("📝 Abrir orquestração", width="stretch",
                     help="Editar como arquivo, salvar na máquina ou abrir um salvo"):
        abrir_janela = "orquestracao"
    if ss.open_orch_after:
        ss.open_orch_after = False
        abrir_janela = "orquestracao"

    t_in, t_conf, t_opt = st.tabs(["📥 Entradas de dados", "🔧 Configurações Spark", "⚙️ Opções"])

    with t_in:
        st.caption("Cada linha vira uma *temp view* que você usa pelo **alias** na query. "
                   "`catalog` lê do Glue Data Catalog (as mesmas tabelas do Athena); `s3` lê arquivos direto do bucket. "
                   "Marque **Cache** nas bases que você consulta várias vezes: elas são lidas uma vez só e ficam em "
                   "memória na sessão para as próximas execuções.")
        ss.inputs_df = st.data_editor(
            ss.inputs_base, num_rows="dynamic", width="stretch", key=f"inputs_editor_{ss.orch_version}",
            column_config={
                "alias": st.column_config.TextColumn("Alias (view)", required=True),
                "kind": st.column_config.SelectboxColumn("Tipo", options=["catalog", "s3"], required=True,
                                                         default="catalog"),
                "source": st.column_config.TextColumn("Origem", help="database.tabela ou s3://bucket/caminho/",
                                                      width="large"),
                "fmt": st.column_config.SelectboxColumn("Formato (s3)", options=["parquet", "csv", "json", "orc",
                                                                                 "delta", "iceberg"],
                                                        default="parquet"),
                "options": st.column_config.TextColumn("Opções (s3)",
                                                       help="chave=valor separados por ; — ex: header=true;sep=;"),
                "filter": st.column_config.TextColumn("Filtro",
                                                      help="Aplicado antes de criar a view. Ex: dt >= '2026-10-01'"),
                "cache": st.column_config.CheckboxColumn("Cache", default=False,
                                                         help="Lê uma vez e mantém em memória entre as execuções"),
            })

    with t_conf:
        st.caption("Aplicadas com `spark.conf.set` antes da query.")
        ss.conf_df = st.data_editor(ss.conf_base, num_rows="dynamic", width="stretch",
                                    key=f"conf_editor_{ss.orch_version}",
                                    column_config={"chave": st.column_config.TextColumn("Chave", width="large"),
                                                   "valor": st.column_config.TextColumn("Valor")})

    with t_opt:
        c1, c2 = st.columns(2)
        ss.default_db = c1.text_input(
            "Database padrão (USE)", ss.default_db, placeholder="opcional",
            help="Usado nas tabelas escritas sem database e no DDL. Vem da configuração (⚙️) e pode ficar vazio. Se "
                 "ele não existir ou a role não tiver acesso, a query roda assim mesmo e o log avisa.").strip()
        ss.row_limit = c2.number_input("Linhas exibidas", 1, 10000, int(ss.row_limit),
                                       help="Apenas estas linhas são trazidas para a tela; a query roda inteira "
                                            "no cluster. Para baixar todas as linhas, use **📦 Preparar CSV com "
                                            "todas as linhas** no resultado.")
        ss.check_refs = st.checkbox(
            "Bloquear a execução se a query usar uma base que não está na orquestração", ss.check_refs,
            help="A conferência é feita antes de enviar ao cluster. Desligue só se ela acusar uma base por engano.")
        ss.profile = st.checkbox(
            "Calcular estatísticas para os metadados (nulos, distintos, mínimo e máximo) no resultado completo",
            ss.profile, help="Roda uma agregação extra sobre o resultado inteiro no cluster: a execução fica "
                             "mais lenta.")
        ss.allow_writes = st.checkbox(
            "Permitir comandos que gravam ou alteram dados (INSERT, CREATE TABLE, DROP, ALTER, MERGE…)",
            ss.allow_writes,
            help="Desligado (padrão), a ferramenta funciona em modo somente leitura: só roda consultas e comandos "
                 "que afetam a própria sessão, como CREATE TEMP VIEW e SET. A role do Glue costuma ter permissão "
                 "de escrita, então um comando esquecido na query gravaria de verdade.")

    if abrir_janela == "athena":
        athena_dialog()
    elif abrir_janela == "orquestracao":
        open_orchestration_dialog()

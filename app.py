"""
Spark Query Studio — frontend para rodar queries Spark SQL na AWS (Glue Interactive Sessions)
e ver as primeiras linhas do resultado, sem precisar passar pelo Athena.

Rodar:  streamlit run app.py
"""
from __future__ import annotations

import math
import os
import re
import time

import pandas as pd
import streamlit as st

import athena_import
import metadata
import orchestration_io
import settings_store
from code_builder import (DEFAULT_LIMIT, InputSource, Orchestration, build_spark_code,
                          split_statements, validate_orchestration)
from engines import (GLUE_VERSIONS, GLUE_WORKER_TYPES, TERMINAL_SESSION_STATES, AwsAccount, EngineError,
                     GlueSessionEngine, InstanceConfig, LocalSparkEngine)
from sql_refs import check_query_against_orchestration, find_write_statements, missing_from_spark_error

st_ace = None
if not os.environ.get("SQS_PLAIN_EDITOR"):   # SQS_PLAIN_EDITOR=1 força o editor simples (usado nos testes)
    try:  # editor com destaque de sintaxe (opcional)
        from streamlit_ace import st_ace
    except ImportError:  # pragma: no cover
        pass

st.set_page_config(page_title="Spark Query Studio", page_icon="⚡", layout="wide")

# ------------------------------------------------------------------------------------
# Estilo
# ------------------------------------------------------------------------------------
st.markdown("""
<style>
.block-container {padding-top: 1.6rem; max-width: 1400px;}
.sqs-header {display:flex; align-items:center; justify-content:space-between; margin-bottom:.4rem;}
.sqs-title {font-size:1.7rem; font-weight:700; letter-spacing:-.02em;}
.sqs-sub {opacity:.65; font-size:.92rem; margin-top:-.2rem;}
.sqs-pill {padding:.28rem .75rem; border-radius:999px; font-size:.8rem; font-weight:600;
           border:1px solid rgba(128,128,128,.3);}
.sqs-ready {background:rgba(34,197,94,.15); color:#16a34a;}
.sqs-off {background:rgba(148,163,184,.15); color:#64748b;}
.sqs-busy {background:rgba(234,179,8,.15); color:#ca8a04;}
.sqs-step {font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.08em;
           color:#f97316; margin-bottom:-.6rem;}
div[data-testid="stMetricValue"] {font-size:1.3rem;}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------------
# Estado
# ------------------------------------------------------------------------------------
ss = st.session_state
if "account" not in ss:
    ss.account, ss.instance = settings_store.load()
ss.setdefault("engine_kind", "glue")
ss.setdefault("session_id", None)
ss.setdefault("session_conf", None)   # config usada para criar a sessão ativa
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
ss.setdefault("allow_writes", False)     # modo somente leitura ligado por padrão
ss.setdefault("running_stmt", None)      # {"session": id, "id": n} enquanto uma query roda no Glue
ss.setdefault("engine_error", None)


def make_engine():
    """Cria o motor. Se a configuração AWS estiver inválida (perfil inexistente, região vazia...),
    devolve None e guarda o erro em ss.engine_error, em vez de derrubar a tela."""
    ss.engine_error = None
    if ss.engine_kind == "local":
        return LocalSparkEngine(ss.instance, ss.account)
    try:
        eng = GlueSessionEngine(ss.account, ss.instance)
    except Exception as e:
        ss.engine_error = f"{type(e).__name__}: {e}"
        return None
    if ss.session_id:
        eng.session_id = ss.session_id
    return eng


def instance_signature(inst: InstanceConfig) -> str:
    return f"{inst.num_workers} x {inst.worker_type} · Glue {inst.glue_version}"


def remember_session(session_id: str):
    """Guarda o ID da sessão Glue assim que ela é criada (antes de esperar ficar pronta)."""
    ss.session_id, ss.session_conf = session_id, instance_signature(ss.instance)


def server_is_local_only() -> bool:
    endereco = (st.get_option("server.address") or "").strip().lower()
    return endereco in ("localhost", "127.0.0.1", "::1") or endereco.startswith("unix://")


REGION_RE = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d+$")


def aws_profiles() -> list[str]:
    try:
        import boto3
        return sorted(boto3.session.Session().available_profiles)
    except Exception:
        return []


def clean(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v).strip()


def current_orchestration() -> Orchestration:
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


INPUT_COLUMNS = ["alias", "kind", "source", "fmt", "options", "filter", "cache"]


def apply_orchestration(orch: Orchestration, keep_conf: bool = False):
    """Substitui a orquestração da tela (os editores são recriados com os novos valores)."""
    ss.inputs_base = pd.DataFrame([{c: getattr(i, c) for c in INPUT_COLUMNS} for i in orch.inputs],
                                  columns=INPUT_COLUMNS)
    if not keep_conf:
        ss.conf_base = pd.DataFrame([{"chave": k, "valor": v} for k, v in orch.spark_conf.items()],
                                    columns=["chave", "valor"])
        ss.row_limit = int(orch.limit)
    ss.default_db = orch.default_database
    ss.orch_version += 1


# ------------------------------------------------------------------------------------
# Diálogo: orquestração (editar como JSON, salvar na máquina, abrir arquivo)
# ------------------------------------------------------------------------------------
@st.dialog("Orquestração", width="large")
def orchestration_dialog():
    st.caption("Ajuste o que for preciso e clique em **Aplicar**. Use **Salvar na máquina** para guardar o "
               "arquivo e **Abrir arquivo** para carregar uma orquestração salva.")
    up = st.file_uploader("Abrir arquivo", type=["json"], key=f"orch_file_{ss.orch_draft_version}")
    if up is not None:
        ss.orch_draft = up.getvalue().decode("utf-8", errors="replace")
        ss.orch_draft_version += 1
        st.rerun(scope="fragment")

    key = f"orch_text_{ss.orch_draft_version}"
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
    ss.orch_draft = orchestration_io.to_json(current_orchestration())
    ss.orch_draft_version += 1
    orchestration_dialog()


# ------------------------------------------------------------------------------------
# Diálogo: importar query do Athena
# ------------------------------------------------------------------------------------
@st.dialog("Importar query do Athena", width="large")
def athena_dialog():
    if not athena_import.sqlglot_available():
        st.error("Para importar queries do Athena instale o pacote **sqlglot** (`pip install sqlglot`).")
        return
    st.caption("Cole a query do Athena ou abra o arquivo. A orquestração é montada a partir das bases usadas e dos "
               "filtros de partição das CTEs, e a query é traduzida para Spark SQL.")
    up = st.file_uploader("Abrir arquivo", type=["sql", "txt"], key="athena_file")
    inicial = up.getvalue().decode("utf-8", errors="replace") if up is not None else ""
    sql_athena = st.text_area("Query do Athena", inicial, height=260, key=f"athena_sql_{up.file_id if up else 0}")
    c1, c2 = st.columns(2)
    db_padrao = c1.text_input("Database padrão", ss.default_db,
                              help="Usado para tabelas escritas sem database na query.")
    usar_glue = c2.checkbox("Consultar o Glue para saber as colunas de partição", ss.engine_kind == "glue",
                            help="Sem o Glue, as colunas de partição são supostas pelo nome (dt, anomes...).")

    if st.button("🔎 Gerar orquestração", type="primary", disabled=not sql_athena.strip()):
        lookup = None
        if usar_glue:
            try:
                lookup = athena_import.glue_partition_lookup(ss.account.boto_session().client("glue"))
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
    if st.button("✅ Aplicar e revisar a orquestração", type="primary"):
        apply_orchestration(r.orchestration, keep_conf=True)
        ss.sql = r.spark_sql
        ss.editor_version += 1
        ss.athena_result = None
        ss.open_orch_after = True
        st.rerun()


# ------------------------------------------------------------------------------------
# Diálogo: conta AWS
# ------------------------------------------------------------------------------------
@st.dialog("Configurar conta AWS", width="large")
def aws_dialog():
    acc: AwsAccount = ss.account
    c1, c2 = st.columns(2)
    region = c1.text_input("Região", acc.region, help="Ex: us-east-1, sa-east-1")
    auth_mode = c2.selectbox(
        "Autenticação", ["profile", "keys", "default"],
        index=["profile", "keys", "default"].index(acc.auth_mode),
        format_func={"profile": "Perfil do AWS CLI (~/.aws)", "keys": "Access key / Secret key",
                     "default": "Cadeia padrão (env vars, SSO, role da instância)"}.get)
    profile, ak, sk, tok = acc.profile, acc.access_key, acc.secret_key, acc.session_token
    if auth_mode == "profile":
        profile = st.text_input("Nome do perfil", acc.profile, placeholder="default")
    elif auth_mode == "keys":
        k1, k2 = st.columns(2)
        ak = k1.text_input("Access key ID", acc.access_key)
        sk = k2.text_input("Secret access key", acc.secret_key, type="password")
        tok = st.text_input("Session token (opcional)", acc.session_token, type="password")
        st.caption("🔒 As chaves ficam só na memória desta sessão e nunca são gravadas em disco.")
    st.divider()
    role = st.text_input("ARN da IAM Role do Glue", acc.glue_role_arn,
                         placeholder="arn:aws:iam::123456789012:role/GlueInteractiveSessionRole",
                         help="Role que o Glue assume para rodar a sessão Spark (precisa ler S3 e Glue Catalog).")
    temp = st.text_input("Diretório temporário no S3 (opcional)", acc.temp_dir, placeholder="s3://meu-bucket/glue-tmp/")

    new = AwsAccount(region=region.strip(), auth_mode=auth_mode, profile=profile.strip(),
                     access_key=ak.strip(), secret_key=sk.strip(), session_token=tok.strip(),
                     glue_role_arn=role.strip(), temp_dir=temp.strip())

    problemas = []
    if not REGION_RE.match(new.region):
        problemas.append("Informe uma região válida, por exemplo `sa-east-1` ou `us-east-1`.")
    if auth_mode == "profile" and new.profile:
        perfis = aws_profiles()
        if perfis and new.profile not in perfis:
            problemas.append(f"O perfil `{new.profile}` não existe no AWS CLI deste computador. "
                             f"Perfis encontrados: {', '.join(perfis)}.")
    for p in problemas:
        st.warning(p)

    b1, b2, _ = st.columns([1, 1, 2])
    if b1.button("Testar conexão", width="stretch", disabled=bool(problemas)):
        try:
            ident = GlueSessionEngine(new, ss.instance).whoami()
            st.success(f"Conectado como `{ident['Arn']}` (conta {ident['Account']})")
        except Exception as e:
            st.error(f"Falha: {e}")
    if b2.button("Salvar", type="primary", width="stretch", disabled=bool(problemas)):
        ss.account = new
        path = settings_store.save(ss.account, ss.instance)
        st.toast(f"Configuração salva em {path}", icon="✅")
        st.rerun()


# ------------------------------------------------------------------------------------
# Sidebar: motor + instância + sessão
# ------------------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### ⚡ Motor Spark")
    ss.engine_kind = st.radio(
        "Onde executar", ["glue", "local"], horizontal=True,
        index=["glue", "local"].index(ss.engine_kind),
        format_func={"glue": "AWS Glue", "local": "Spark local"}.get, label_visibility="collapsed")

    if ss.engine_kind == "glue":
        if st.button("🔑 Configurar conta AWS", width="stretch"):
            aws_dialog()
        acc = ss.account
        st.caption(f"Região **{acc.region}** · auth **{acc.auth_mode}**"
                   + ("" if acc.glue_role_arn else " · ⚠️ role não definida"))

        st.markdown("### 🖥️ Instância")
        inst: InstanceConfig = ss.instance
        inst.worker_type = st.selectbox(
            "Tipo de worker", list(GLUE_WORKER_TYPES),
            index=list(GLUE_WORKER_TYPES).index(inst.worker_type) if inst.worker_type in GLUE_WORKER_TYPES else 1,
            format_func=lambda w: f"{w} — {GLUE_WORKER_TYPES[w]}")
        inst.num_workers = st.slider("Número de workers", 2, 100, int(inst.num_workers))
        c1, c2 = st.columns(2)
        inst.glue_version = c1.selectbox("Versão Glue", GLUE_VERSIONS,
                                         index=GLUE_VERSIONS.index(inst.glue_version) if inst.glue_version in GLUE_VERSIONS else 0)
        inst.idle_timeout_min = c2.number_input("Idle (min)", 1, 480, int(inst.idle_timeout_min),
                                                help="A sessão é encerrada sozinha após esse tempo sem uso.")
        dpu = {"G.025X": .25, "G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 8, "R.1X": 1, "R.2X": 2}[inst.worker_type]
        st.caption(f"≈ **{dpu * inst.num_workers:g} DPU** enquanto a sessão estiver ativa.")
        with st.expander("Argumentos extras do job"):
            st.caption("Ex: `--datalake-formats` = `iceberg`, `--additional-python-modules` = `pyarrow==14`")
            if "args_base" not in ss:
                ss.args_base = pd.DataFrame([{"argumento": k, "valor": v} for k, v in inst.extra_args.items()]
                                            or [{"argumento": "", "valor": ""}])
            args_df = st.data_editor(ss.args_base, num_rows="dynamic", width="stretch",
                                     key="args_editor")
            inst.extra_args = {clean(r["argumento"]): clean(r["valor"]) for _, r in args_df.iterrows()
                               if clean(r.get("argumento"))}
        if st.button("💾 Salvar instância como padrão", width="stretch"):
            settings_store.save(ss.account, inst)
            st.toast("Instância salva", icon="✅")

    if not server_is_local_only():
        st.warning("⚠️ O app está aceitando conexões de outras máquinas da rede. Abra pelo `run.sh` / "
                   "`run_windows.bat` ou use `streamlit run app.py --server.address localhost`.")

    st.markdown("### 🔌 Sessão")
    engine = make_engine()
    status, status_error = None, None
    if engine is None:
        st.error(f"Não consegui preparar a conexão com a AWS ({ss.engine_error}). "
                 "Revise em **Configurar conta AWS**.")
    elif ss.engine_kind == "glue" and ss.session_id:
        try:
            status = engine.session_status()
        except Exception as e:
            status_error = e           # falha momentânea: a sessão continua registrada
        if status_error is None and (status is None or status in TERMINAL_SESSION_STATES):
            ss.session_id, ss.session_conf, ss.running_stmt = None, None, None
    elif ss.engine_kind == "local":
        status = engine.session_status()

    def _botao_encerrar():
        if st.button("⏹️ Encerrar sessão", width="stretch"):
            try:
                engine.stop()
            except Exception as e:
                st.error(f"Não consegui encerrar a sessão: {e}")
            else:
                ss.session_id, ss.session_conf, ss.running_stmt = None, None, None
                st.rerun()

    if status == "READY":
        label = ss.session_id if ss.engine_kind == "glue" else "local"
        st.success(f"Ativa: `{label}`")
        if ss.engine_kind == "glue" and ss.session_conf and ss.session_conf != instance_signature(ss.instance):
            st.warning(f"A sessão ativa usa **{ss.session_conf}**. Reinicie para aplicar a nova instância.")
        _botao_encerrar()
    elif ss.engine_kind == "glue" and ss.session_id and status_error is not None:
        st.warning(f"Não consegui consultar a sessão `{ss.session_id}` ({type(status_error).__name__}). "
                   "Ela continua registrada; a próxima execução tenta de novo.")
        _botao_encerrar()
    elif ss.engine_kind == "glue" and ss.session_id:
        st.info(f"Sessão `{ss.session_id}` iniciando ({status}). Ela será usada na próxima execução.")
        _botao_encerrar()
    else:
        st.info("Nenhuma sessão ativa. Ela é criada automaticamente na primeira execução "
                "(no Glue isso leva ~30–90 s).")
        if st.button("▶️ Iniciar sessão agora", width="stretch", disabled=engine is None):
            with st.status("Iniciando sessão…", expanded=True) as box:
                try:
                    engine.ensure_session(lambda m: box.update(label=m), on_created=remember_session)
                    if ss.engine_kind == "glue":
                        remember_session(engine.session_id)
                    box.update(label="Sessão pronta", state="complete")
                    st.rerun()
                except Exception as e:
                    box.update(label="Falha ao iniciar", state="error")
                    st.error(str(e))

# ------------------------------------------------------------------------------------
# Cabeçalho
# ------------------------------------------------------------------------------------
if status == "READY":
    pill = ("sqs-ready", "Sessão pronta")
elif ss.engine_kind == "glue" and ss.session_id:
    pill = ("sqs-busy", "Sessão iniciando")
else:
    pill = ("sqs-off", "Sem sessão")
engine_label = (f"AWS Glue · {instance_signature(ss.instance)}" if ss.engine_kind == "glue" else "Spark local")
st.markdown(f"""
<div class="sqs-header">
  <div><div class="sqs-title">⚡ Spark Query Studio</div>
  <div class="sqs-sub">{engine_label}</div></div>
  <span class="sqs-pill {pill[0]}">● {pill[1]}</span>
</div>""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------------
# 1. Orquestração
# ------------------------------------------------------------------------------------
st.markdown('<div class="sqs-step">Passo 1</div>', unsafe_allow_html=True)
st.subheader("Orquestração das entradas")
b_ath, b_orch, _ = st.columns([1, 1, 2])
# As janelas só abrem depois das abas abaixo: assim elas já veem as edições feitas nas tabelas
# (as tabelas só são lidas quando são desenhadas).
abrir_janela = None
if b_ath.button("📥 Importar query do Athena", width="stretch"):
    abrir_janela = "athena"
if b_orch.button("📝 Abrir orquestração", width="stretch", help="Editar como arquivo, salvar na máquina ou abrir um salvo"):
    abrir_janela = "orquestracao"
if ss.open_orch_after:
    ss.open_orch_after = False
    abrir_janela = "orquestracao"

t_in, t_conf, t_opt = st.tabs(["📥 Entradas de dados", "🔧 Configurações Spark", "⚙️ Opções"])

with t_in:
    st.caption("Cada linha vira uma *temp view* que você usa pelo **alias** na query. "
               "`catalog` lê do Glue Data Catalog (as mesmas tabelas do Athena); `s3` lê arquivos direto do bucket.")
    ss.inputs_df = st.data_editor(
        ss.inputs_base, num_rows="dynamic", width="stretch", key=f"inputs_editor_{ss.orch_version}",
        column_config={
            "alias": st.column_config.TextColumn("Alias (view)", required=True),
            "kind": st.column_config.SelectboxColumn("Tipo", options=["catalog", "s3"], required=True, default="catalog"),
            "source": st.column_config.TextColumn("Origem", help="database.tabela ou s3://bucket/caminho/", width="large"),
            "fmt": st.column_config.SelectboxColumn("Formato (s3)", options=["parquet", "csv", "json", "orc", "delta", "iceberg"], default="parquet"),
            "options": st.column_config.TextColumn("Opções (s3)", help="chave=valor separados por ; — ex: header=true;sep=;"),
            "filter": st.column_config.TextColumn("Filtro", help="Aplicado antes de criar a view. Ex: dt >= '2026-10-01'"),
            "cache": st.column_config.CheckboxColumn("Cache", default=False),
        })

with t_conf:
    st.caption("Aplicadas com `spark.conf.set` antes da query.")
    ss.conf_df = st.data_editor(ss.conf_base, num_rows="dynamic", width="stretch",
                                key=f"conf_editor_{ss.orch_version}",
                                column_config={"chave": st.column_config.TextColumn("Chave", width="large"),
                                               "valor": st.column_config.TextColumn("Valor")})

with t_opt:
    c1, c2 = st.columns(2)
    ss.default_db = c1.text_input("Database padrão (USE)", ss.default_db, placeholder="opcional")
    ss.row_limit = c2.number_input("Linhas exibidas", 1, 10000, int(ss.row_limit),
                                   help="Apenas estas linhas são trazidas para a tela; a query roda inteira no cluster.")
    ss.check_refs = st.checkbox(
        "Bloquear a execução se a query usar uma base que não está na orquestração", ss.check_refs,
        help="A conferência é feita antes de enviar ao cluster. Desligue só se ela acusar uma base por engano.")
    ss.profile = st.checkbox(
        "Calcular estatísticas para os metadados (nulos, distintos, mínimo e máximo) no resultado completo",
        ss.profile, help="Roda uma agregação extra sobre o resultado inteiro no cluster: a execução fica mais lenta.")
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

# ------------------------------------------------------------------------------------
# 2. Query
# ------------------------------------------------------------------------------------
st.markdown('<div class="sqs-step">Passo 2</div>', unsafe_allow_html=True)
st.subheader("Query")

up_sql = st.file_uploader("Importar query de arquivo (.sql / .txt) — ou escreva direto no editor abaixo",
                          type=["sql", "txt", "hql"], key="sql_upload")
if up_sql is not None and ss.last_upload != up_sql.file_id:
    ss.sql = up_sql.getvalue().decode("utf-8", errors="replace")
    ss.last_upload = up_sql.file_id
    ss.editor_version += 1          # força o editor a recarregar com o conteúdo importado
    st.rerun()

if st_ace is not None:
    edited = st_ace(value=ss.sql, language="sql", theme="tomorrow_night", font_size=14, height=320,
                    show_gutter=True, wrap=True, auto_update=True, key=f"ace_{ss.editor_version}")
    if edited is not None:
        ss.sql = edited
else:
    ss.sql = st.text_area("Editor", ss.sql, height=320, key=f"ta_{ss.editor_version}",
                          label_visibility="collapsed")

n_stmts = len(split_statements(ss.sql))
st.caption(f"{n_stmts} comando(s). Separe vários com `;` — todos são executados e o resultado do **último** é exibido.")
ref_check = check_query_against_orchestration(ss.sql, current_orchestration().inputs)
if ref_check.missing:
    nomes = ", ".join(f"`{m.name}`" for m in ref_check.missing)
    st.caption(f"⚠️ Fora da orquestração: {nomes}")
escritas = [] if ss.allow_writes else find_write_statements(ss.sql)
if escritas:
    st.caption("🔒 Modo somente leitura: " + ", ".join(f"o comando {w.number} ({w.keyword})" for w in escritas)
               + " grava dados e será bloqueado.")

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
        st.code(build_spark_code(ss.sql, current_orchestration(), profile=ss.profile), language="python")
    except ValueError as e:
        st.caption(str(e))

# ------------------------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------------------------
if run_clicked:
    orch = current_orchestration()
    errors = validate_orchestration(orch)
    if not ss.sql.strip():
        errors.append("A query está vazia.")
    if ss.check_refs and not errors:
        errors += [m.message() for m in ref_check.missing]
    if not errors:
        errors += [w.message() for w in escritas]
    code = None
    if not errors:
        try:
            code = build_spark_code(ss.sql, orch, profile=ss.profile)
        except ValueError as e:
            errors.append(str(e))
    if engine is None and not errors:
        errors.append(f"A conexão com a AWS não está pronta ({ss.engine_error}). Revise em **Configurar conta AWS**.")
    for aviso in ref_check.warnings:
        st.warning(aviso)
    if errors:
        for e in errors:
            st.error(e)
    else:
        ss.result_version += 1
        t0 = time.time()

        def _guardar_statement(statement_id: int):
            ss.running_stmt = {"session": engine.session_id, "id": statement_id}

        with st.status("Preparando…", expanded=False) as box:
            try:
                if ss.engine_kind == "glue":
                    engine.session_id = ss.session_id
                    engine.ensure_session(lambda m: box.update(label=m), on_created=remember_session)
                    if ss.session_id != engine.session_id:
                        remember_session(engine.session_id)
                    # uma execução interrompida (você mexeu na tela no meio) pode ter deixado a query
                    # anterior rodando na sessão; ela atrasaria esta, então é cancelada
                    anterior = ss.running_stmt
                    if anterior and anterior.get("session") == engine.session_id:
                        if engine.cancel_statement(anterior["id"]):
                            box.update(label="A execução anterior, que tinha ficado rodando, foi cancelada")
                    ss.running_stmt = None
                res = engine.run(code, lambda m: box.update(label=m), on_submitted=_guardar_statement)
                ss.running_stmt = None
                res["total"] = round(time.time() - t0, 1)
                ss.result = res
                box.update(label=("Concluído" if res.get("ok") else "Erro na query") + f" em {res['total']}s",
                           state="complete" if res.get("ok") else "error")
            except EngineError as e:
                box.update(label="Falha", state="error")
                ss.result = {"ok": False, "error": str(e), "traceback": "", "log": []}
            except Exception as e:
                box.update(label="Falha", state="error")
                ss.result = {"ok": False, "error": f"{type(e).__name__}: {e}", "traceback": "", "log": []}

def render_metadata(res: dict, col_names: list[str]):
    cols = [dict(c, name=n) for c, n in zip(res["columns"], col_names)]
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


# ------------------------------------------------------------------------------------
# 3. Resultado
# ------------------------------------------------------------------------------------
res = ss.result
if res:
    st.markdown('<div class="sqs-step">Passo 3</div>', unsafe_allow_html=True)
    st.subheader("Resultado")
    if res.get("ok"):
        cols = [c["name"] for c in res["columns"]]
        # nomes de coluna duplicados (ex: JOIN sem alias) quebram o DataFrame
        seen, uniq = {}, []
        for c in cols:
            seen[c] = seen.get(c, 0) + 1
            uniq.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
        df = pd.DataFrame(res["rows"], columns=uniq)

        m1, m2, m3 = st.columns(3)
        m1.metric("Linhas exibidas", f"{len(df):,}".replace(",", "."))
        m2.metric("Colunas", len(df.columns))
        m3.metric("Tempo", f"{res.get('total', res.get('elapsed', 0))} s")

        t_dados, t_meta = st.tabs(["📋 Dados", "🧾 Metadados"])
        with t_dados:
            st.dataframe(df, width="stretch", height=min(38 + 35 * len(df), 560), hide_index=True)

            c1, c2, c3 = st.columns([1, 1, 2])
            sep = c1.selectbox("Separador", [";", ",", "\t"], format_func={";": "; (Excel BR)", ",": ", (padrão)", "\t": "tab"}.get)
            csv_bytes = df.to_csv(index=False, sep=sep).encode("utf-8-sig")
            c2.markdown("<div style='height:1.75rem'></div>", unsafe_allow_html=True)
            c2.download_button("⬇️ Exportar CSV", csv_bytes, f"resultado_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                               "text/csv", type="primary", width="stretch")

        with t_meta:
            render_metadata(res, uniq)
    else:
        faltando = missing_from_spark_error(res.get("error", ""))
        if faltando:
            tipo, nome = faltando
            if tipo == "tabela":
                st.error(f"A base `{nome}` não foi encontrada pelo Spark. Ela precisa estar na orquestração: "
                         f"adicione-a em Passo 1 › Entradas de dados.")
            else:
                st.error(f"O banco `{nome}` não foi encontrado pelo Spark. Confira o nome na orquestração "
                         f"(Passo 1 › Entradas de dados) e o database padrão.")
            with st.expander("Mensagem original do Spark"):
                st.code(res.get("error", ""), language="text")
        else:
            st.error(res.get("error", "Erro desconhecido"))
        if res.get("traceback"):
            with st.expander("Detalhes do erro"):
                st.code(res["traceback"], language="text")

    if res.get("log"):
        with st.expander("Log de orquestração"):
            st.code("\n".join(res["log"]), language="text")

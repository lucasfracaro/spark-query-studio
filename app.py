"""
Spark Query Studio — frontend para rodar queries Spark SQL na AWS (Glue Interactive Sessions)
e ver as primeiras linhas do resultado, sem precisar passar pelo Athena.

Rodar:  streamlit run app.py
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import asdict

import pandas as pd
import streamlit as st

import settings_store
from code_builder import (DEFAULT_LIMIT, InputSource, Orchestration, build_spark_code,
                          split_statements, validate_orchestration)
from engines import (GLUE_VERSIONS, GLUE_WORKER_TYPES, AwsAccount, EngineError,
                     GlueSessionEngine, InstanceConfig, LocalSparkEngine)

try:  # editor com destaque de sintaxe (opcional)
    from streamlit_ace import st_ace
except ImportError:  # pragma: no cover
    st_ace = None

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


def make_engine():
    if ss.engine_kind == "local":
        return LocalSparkEngine(ss.instance, ss.account)
    eng = GlueSessionEngine(ss.account, ss.instance)
    if ss.session_id:
        eng.session_id = ss.session_id
    return eng


def instance_signature(inst: InstanceConfig) -> str:
    return f"{inst.num_workers} x {inst.worker_type} · Glue {inst.glue_version}"


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
    b1, b2, _ = st.columns([1, 1, 2])
    if b1.button("Testar conexão", use_container_width=True):
        try:
            ident = GlueSessionEngine(new, ss.instance).whoami()
            st.success(f"Conectado como `{ident['Arn']}` (conta {ident['Account']})")
        except Exception as e:
            st.error(f"Falha: {e}")
    if b2.button("Salvar", type="primary", use_container_width=True):
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
        if st.button("🔑 Configurar conta AWS", use_container_width=True):
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
            args_df = st.data_editor(ss.args_base, num_rows="dynamic", use_container_width=True,
                                     key="args_editor")
            inst.extra_args = {clean(r["argumento"]): clean(r["valor"]) for _, r in args_df.iterrows()
                               if clean(r.get("argumento"))}
        if st.button("💾 Salvar instância como padrão", use_container_width=True):
            settings_store.save(ss.account, inst)
            st.toast("Instância salva", icon="✅")

    st.markdown("### 🔌 Sessão")
    engine = make_engine()
    status = None
    if ss.engine_kind == "glue" and ss.session_id:
        try:
            status = engine.session_status()
        except Exception as e:
            st.warning(f"Não consegui consultar a sessão: {e}")
        if status != "READY":
            ss.session_id, ss.session_conf = None, None
    elif ss.engine_kind == "local":
        status = engine.session_status()

    if status == "READY":
        label = ss.session_id if ss.engine_kind == "glue" else "local"
        st.success(f"Ativa: `{label}`")
        if ss.engine_kind == "glue" and ss.session_conf and ss.session_conf != instance_signature(ss.instance):
            st.warning(f"A sessão ativa usa **{ss.session_conf}**. Reinicie para aplicar a nova instância.")
        if st.button("⏹️ Encerrar sessão", use_container_width=True):
            engine.stop()
            ss.session_id, ss.session_conf = None, None
            st.rerun()
    else:
        st.info("Nenhuma sessão ativa. Ela é criada automaticamente na primeira execução "
                "(no Glue isso leva ~30–90 s).")
        if st.button("▶️ Iniciar sessão agora", use_container_width=True):
            with st.status("Iniciando sessão…", expanded=True) as box:
                try:
                    sid = engine.start(lambda m: box.update(label=m))
                    if ss.engine_kind == "glue":
                        ss.session_id, ss.session_conf = sid, instance_signature(ss.instance)
                    box.update(label="Sessão pronta", state="complete")
                    st.rerun()
                except Exception as e:
                    box.update(label="Falha ao iniciar", state="error")
                    st.error(str(e))

# ------------------------------------------------------------------------------------
# Cabeçalho
# ------------------------------------------------------------------------------------
pill = ("sqs-ready", "Sessão pronta") if status == "READY" else ("sqs-off", "Sem sessão")
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
t_in, t_conf, t_opt, t_io = st.tabs(["📥 Entradas de dados", "🔧 Configurações Spark", "⚙️ Opções", "📁 Importar / exportar"])

with t_in:
    st.caption("Cada linha vira uma *temp view* que você usa pelo **alias** na query. "
               "`catalog` lê do Glue Data Catalog (as mesmas tabelas do Athena); `s3` lê arquivos direto do bucket.")
    ss.inputs_df = st.data_editor(
        ss.inputs_base, num_rows="dynamic", use_container_width=True, key=f"inputs_editor_{ss.orch_version}",
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
    ss.conf_df = st.data_editor(ss.conf_base, num_rows="dynamic", use_container_width=True,
                                key=f"conf_editor_{ss.orch_version}",
                                column_config={"chave": st.column_config.TextColumn("Chave", width="large"),
                                               "valor": st.column_config.TextColumn("Valor")})

with t_opt:
    c1, c2 = st.columns(2)
    ss.default_db = c1.text_input("Database padrão (USE)", ss.default_db, placeholder="opcional")
    ss.row_limit = c2.number_input("Linhas exibidas", 1, 10000, int(ss.row_limit),
                                   help="Apenas estas linhas são trazidas para a tela; a query roda inteira no cluster.")

with t_io:
    st.caption("Salve a orquestração (entradas + configs) em JSON para reutilizar ou compartilhar.")
    orch_json = json.dumps({"inputs": ss.inputs_df.to_dict("records"), "conf": ss.conf_df.to_dict("records"),
                            "default_db": ss.default_db, "limit": int(ss.row_limit)}, indent=2, default=str)
    c1, c2 = st.columns(2)
    c1.download_button("⬇️ Exportar orquestração", orch_json, "orquestracao.json", "application/json",
                       use_container_width=True)
    up = c2.file_uploader("Importar orquestração", type=["json"], label_visibility="collapsed", key="orch_upload")
    if up is not None and ss.get("last_orch") != up.file_id:
        data = json.loads(up.getvalue())
        cols_in = ["alias", "kind", "source", "fmt", "options", "filter", "cache"]
        ss.inputs_base = pd.DataFrame(data.get("inputs", []), columns=cols_in)
        ss.conf_base = pd.DataFrame(data.get("conf", []), columns=["chave", "valor"])
        ss.orch_version += 1
        ss.default_db, ss.row_limit = data.get("default_db", ""), int(data.get("limit", DEFAULT_LIMIT))
        ss.last_orch = up.file_id
        st.rerun()

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

b1, b2, b3, _ = st.columns([1.2, 1, 1, 3])
run_clicked = b1.button("▶️ Executar", type="primary", use_container_width=True)
b2.download_button("💾 Salvar .sql", ss.sql, "query.sql", "text/plain", use_container_width=True)
if b3.button("🧹 Limpar", use_container_width=True):
    ss.sql, ss.result = "", None
    ss.editor_version += 1
    st.rerun()

with st.expander("Ver código PySpark gerado"):
    try:
        st.code(build_spark_code(ss.sql, current_orchestration()), language="python")
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
    if errors:
        for e in errors:
            st.error(e)
    else:
        code = build_spark_code(ss.sql, orch)
        engine = make_engine()
        t0 = time.time()
        with st.status("Preparando…", expanded=False) as box:
            try:
                if ss.engine_kind == "glue" and not ss.session_id:
                    ss.session_id = engine.start(lambda m: box.update(label=m))
                    ss.session_conf = instance_signature(ss.instance)
                res = engine.run(code, lambda m: box.update(label=m))
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

        st.dataframe(df, use_container_width=True, height=min(38 + 35 * len(df), 560), hide_index=True)

        c1, c2, c3 = st.columns([1, 1, 2])
        sep = c1.selectbox("Separador", [";", ",", "\t"], format_func={";": "; (Excel BR)", ",": ", (padrão)", "\t": "tab"}.get)
        csv_bytes = df.to_csv(index=False, sep=sep).encode("utf-8-sig")
        c2.markdown("<div style='height:1.75rem'></div>", unsafe_allow_html=True)
        c2.download_button("⬇️ Exportar CSV", csv_bytes, f"resultado_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                           "text/csv", type="primary", use_container_width=True)

        with st.expander("Schema"):
            st.dataframe(pd.DataFrame(res["columns"]).rename(columns={"name": "coluna", "type": "tipo"}),
                         hide_index=True, use_container_width=True)
    else:
        st.error(res.get("error", "Erro desconhecido"))
        if res.get("traceback"):
            with st.expander("Detalhes do erro"):
                st.code(res["traceback"], language="text")

    if res.get("log"):
        with st.expander("Log de orquestração"):
            st.code("\n".join(res["log"]), language="text")

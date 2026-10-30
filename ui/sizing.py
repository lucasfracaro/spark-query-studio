"""Estimativa do tamanho da sessão (tipo e número de workers) na importação do Athena, e a sugestão na
barra lateral. A coleta (Glue, S3, histórico do Athena) está em sizing.py; aqui só a tela."""
from __future__ import annotations

import pandas as pd
import streamlit as st

import sizing
from sizing import fmt_bytes, fmt_count, fmt_duration
from ui.engine import reader
from ui.state import clean

TARGETS = {60: "~1 min", 120: "~2 min", 300: "~5 min", 600: "~10 min"}
SHUFFLE_KEY = "spark.sql.shuffle.partitions"


def options():
    """Campos opcionais da busca no histórico do Athena (na janela de importação)."""
    with st.expander("Histórico do Athena para a estimativa (opcional)"):
        st.caption("O app procura esta query no histórico do Athena para usar os números da execução: dados lidos, "
                   "linhas e troca entre as etapas. Ele só lê o histórico: não roda nada no Athena nem ocupa sessão.")
        c1, c2 = st.columns(2)
        c1.text_input("Workgroup do Athena", key="sizing_wg", placeholder="todos os que você acessa",
                      help="Informar o workgroup deixa a busca mais rápida.")
        c2.text_input("ID de uma execução desta query", key="sizing_exec", placeholder="opcional",
                      help="No console do Athena: Recent queries → copie o Query ID. Com o ID, o app não precisa "
                           "procurar no histórico.")


def collect(result, sql_athena: str) -> sizing.SizingData:
    """Consulta o Glue, o S3 (só buckets da sua conta) e o histórico do Athena. Leva alguns segundos."""
    ss = st.session_state
    clientes, notas = {}, []
    for servico in ("glue", "s3", "athena"):
        try:
            clientes[servico] = ss.account.client(servico)
        except Exception as e:
            clientes[servico] = None
            notas.append(f"Não consegui conectar ao {servico} ({type(e).__name__}: {e}).")
    dados = sizing.collect(result.tables, sql_athena, glue=clientes["glue"], s3=clientes["s3"],
                           athena=clientes["athena"], workgroup=ss.get("sizing_wg", ""),
                           execution_id=ss.get("sizing_exec", ""))
    dados.notes[:0] = notas
    return dados


def render(dados: sizing.SizingData) -> sizing.Estimate:
    """Mostra a estimativa (refeita na hora quando muda o tempo desejado) e devolve o resultado."""
    ss = st.session_state
    st.markdown("##### 📏 Tamanho sugerido para a sessão")
    c1, c2 = st.columns([3, 1])
    alvo = c2.selectbox("Processar em", list(TARGETS), index=1, format_func=TARGETS.get, key="sizing_target",
                        help="Mais rápido pede mais workers. A sessão cobra pelo tamanho enquanto estiver ativa, "
                             "mesmo parada esperando a próxima query.")
    est = sizing.estimate(dados, reader(), alvo)
    ss.sizing = est
    if est.ok:
        c1.success(f"**{est.label}** · ≈ {est.dpu:g} DPU · processamento {fmt_duration(est.seconds)} (fora a subida "
                   f"da sessão) · confiança **{est.confidence}**")
    else:
        c1.warning("Não deu para estimar o tamanho da sessão.")
    st.markdown("\n".join(f"- {r}" for r in est.reasons))
    if est.alternative is not None:
        alt = est.alternative
        st.caption(f"Glue 4.0 com Lake Formation: se o log da execução mostrar que a leitura caiu no DynamicFrame "
                   f"(lê todas as colunas, ~{fmt_bytes(alt.read_bytes)}), use **{alt.label}** "
                   f"(processamento {fmt_duration(alt.seconds)}).")
    if dados.athena is not None:
        st.caption("🗂️ " + dados.athena.describe())
    tabela = _tabela(dados.tables)
    if tabela is not None:
        st.dataframe(tabela, hide_index=True, width="stretch")
    for nota in est.notes + dados.notes:
        st.caption("ℹ️ " + nota)
    with st.expander("Como a conta é feita"):
        st.markdown(_metodo())
    if est.ok:
        st.checkbox(f"Usar **{est.label}** na sessão ao aplicar", value=True, key="sizing_apply",
                    help="Se já houver uma sessão ativa com outro tamanho, ela continua; o novo tamanho vale para a "
                         "próxima sessão (o painel da sessão avisa).")
        if est.shuffle_partitions:
            st.checkbox(f"Pôr `{SHUFFLE_KEY}` = {est.shuffle_partitions} nas configurações Spark (com o padrão, 200, "
                        f"cada tarefa do shuffle teria ~{fmt_bytes(est.shuffle_bytes / 200)})",
                        value=True, key="sizing_conf")
    return est


def apply(est: sizing.Estimate | None):
    """Ao aplicar a orquestração importada: o tamanho sugerido na sessão e as partições do shuffle."""
    ss = st.session_state
    if est is None or not est.ok:
        return
    if ss.get("sizing_apply", True):
        ss.instance.worker_type, ss.instance.num_workers = est.worker_type, est.num_workers
    if est.shuffle_partitions and ss.get("sizing_conf", True):
        outras = [{"chave": clean(r.get("chave")), "valor": clean(r.get("valor"))} for _, r in ss.conf_base.iterrows()
                  if clean(r.get("chave")) and clean(r.get("chave")) != SHUFFLE_KEY]
        ss.conf_base = pd.DataFrame([{"chave": SHUFFLE_KEY, "valor": str(est.shuffle_partitions)}, *outras],
                                    columns=["chave", "valor"])


def sidebar_hint(inst):
    """Na barra lateral: a última estimativa, se ela for diferente do que está configurado."""
    est = st.session_state.get("sizing")
    if est is None or not est.ok or (est.worker_type, est.num_workers) == (inst.worker_type, inst.num_workers):
        return
    c1, c2 = st.columns([3, 1], vertical_alignment="center")
    c1.caption(f"📏 Estimativa da importação do Athena: **{est.label}** (confiança {est.confidence})")
    if c2.button("Usar", key="usar_estimativa", width="stretch",
                 help="Passa a usar o tipo e o número de workers estimados"):
        inst.worker_type, inst.num_workers = est.worker_type, est.num_workers
        st.rerun()


def _tabela(tabelas: list[sizing.TableSize]) -> pd.DataFrame | None:
    linhas = []
    for t in tabelas:
        if not t.partitioned:
            particoes = "sem partição" if t.columns_total is not None else "—"
        elif t.partitions is None:
            particoes = "—"
        else:
            particoes = f"{t.partitions}" + (f" de {t.partitions_total}" if t.partitions_total else "")
        colunas = (f"{t.columns_used} de {t.columns_total}" if t.columns_total and t.columns_used is not None
                   else "—")
        linhas.append({"alias": t.alias, "partições": particoes, "tamanho (todas as colunas)": fmt_bytes(t.bytes),
                       "colunas usadas": colunas, "leitura estimada": fmt_bytes(t.read_bytes),
                       "linhas": fmt_count(t.rows), "tamanho veio de": t.bytes_source or "—",
                       "observações": "; ".join(t.notes) or "—"})
    return pd.DataFrame(linhas) if linhas else None


def _metodo() -> str:
    return (
        f"- **Leitura:** ~{fmt_bytes(sizing.READ_RATE)}/s por núcleo (Parquet/ORC comprimido, só as colunas usadas); "
        f"~{fmt_bytes(sizing.READ_RATE_DYNAMIC_FRAME)}/s pelo DynamicFrame, que lê todas as colunas. Vem da execução "
        "da mesma query no Athena (dados lidos) ou do tamanho das partições escolhidas × a parte das colunas que a "
        "query usa (texto pesa mais que número).\n"
        f"- **Arquivos:** o Spark divide os arquivos em pedaços de {fmt_bytes(sizing.SPLIT_BYTES)} pelo tamanho "
        f"inteiro, mesmo lendo só algumas colunas; cada pedaço custa ~{str(sizing.TASK_COST_S).replace('.', ',')} s "
        "(abrir, ler o rodapé "
        "do Parquet, latência do S3).\n"
        f"- **Shuffle:** ~{fmt_bytes(sizing.SHUFFLE_RATE)}/s por núcleo. Vem do Athena (o que as etapas passaram "
        "umas para as outras) ou da query: leve 5%, média 50% e pesada 120% dos dados lidos já descomprimidos "
        f"(~{sizing.EXPANSION:g}× o Parquet).\n"
        "- **Workers:** os núcleos que terminam no tempo escolhido, sem passar do paralelismo que os dados rendem. "
        "Cada worker do Glue é 1 executor e 1 worker é o driver. O shuffle precisa caber no disco livre dos workers "
        "(G.1X ~44 GB, G.2X ~78 GB, G.4X ~230 GB, G.8X ~485 GB).\n"
        "- **Tipo:** G.1X por padrão (mesmo preço por DPU e mais executores pelo mesmo custo); G.2X para query pesada "
        "com mais de 20 GB ou com muitas partições/arquivos (driver maior); G.4X/G.8X para volumes de TB.\n"
        "- Os números são aproximados: servem para acertar a ordem de grandeza. Depois da primeira execução, o tempo "
        "real mostra se vale subir ou descer."
    )

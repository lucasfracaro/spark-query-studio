"""
Spark Query Studio — frontend para rodar queries Spark SQL na AWS (Glue Interactive Sessions)
e ver as primeiras linhas do resultado, sem precisar passar pelo Athena.

Rodar:  streamlit run app.py   (ou run.sh / run_windows.bat)

As partes da tela ficam em ui/; aqui fica só a ordem em que elas aparecem.
"""
from __future__ import annotations

import streamlit as st

from session_hub import HUB
from ui import orchestration, query, results, sidebar, state, style
from ui.config_dialog import open_config_dialog

st.set_page_config(page_title="Spark Query Studio", page_icon="⚡", layout="wide")
style.apply()
state.init()
ss = st.session_state

# ------------------------------------------------------------------------------------
# Barra lateral: motor, configuração, instância e sessão
# ------------------------------------------------------------------------------------
engine = sidebar.render()

# ------------------------------------------------------------------------------------
# Cabeçalho
# ------------------------------------------------------------------------------------
status, iniciando = None, False
if engine is not None:
    slot = HUB.slot(engine)
    try:
        status = HUB.status(engine)
    except Exception:
        status = slot.status
    iniciando = engine.kind == "glue" and (slot.starting or bool(slot.session_id))
if status == "READY":
    pill = ("sqs-ready", "Sessão pronta")
elif iniciando:
    pill = ("sqs-busy", "Sessão iniciando")
else:
    pill = ("sqs-off", "Sem sessão")
engine_label = f"AWS Glue · {ss.instance.signature()}" if ss.engine_kind == "glue" else "Spark local"
st.markdown(f"""
<div class="sqs-header">
  <div><div class="sqs-title">⚡ Spark Query Studio</div>
  <div class="sqs-sub">{engine_label}</div></div>
  <span class="sqs-pill {pill[0]}">● {pill[1]}</span>
</div>""", unsafe_allow_html=True)

# Problemas no arquivo de configuração: o app abre com os padrões e mostra o que corrigir
lc = ss.config_loaded
if lc.error or lc.problems:
    with st.container(border=True):
        st.warning(f"**A configuração em `{lc.path.name}` precisa de ajuste.** "
                   "Onde havia erro, o app está usando o valor padrão.")
        for problema in ([lc.error] if lc.error else []) + lc.problems[:6]:
            st.markdown(f"- {problema}")
        if st.button("⚙️ Abrir a configuração para corrigir", key="corrigir_config"):
            open_config_dialog()

# ------------------------------------------------------------------------------------
# 1. Orquestração · 2. Query (e execução) · 3. Resultado
# ------------------------------------------------------------------------------------
orchestration.render()
query.render(engine)
results.render(engine)

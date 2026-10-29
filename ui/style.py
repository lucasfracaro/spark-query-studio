"""Estilo da página."""
import streamlit as st

CSS = """
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
"""


def apply():
    st.markdown(CSS, unsafe_allow_html=True)


def step(numero: int):
    st.markdown(f'<div class="sqs-step">Passo {numero}</div>', unsafe_allow_html=True)

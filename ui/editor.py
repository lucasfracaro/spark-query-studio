"""Editor de código com destaque de sintaxe (streamlit-ace), quando instalado."""
from __future__ import annotations

import os


def ace():
    """A função st_ace, ou None para usar a caixa de texto simples.
    SQS_PLAIN_EDITOR=1 força a caixa simples (usado nos testes)."""
    if os.environ.get("SQS_PLAIN_EDITOR"):
        return None
    try:
        from streamlit_ace import st_ace
    except ImportError:  # pragma: no cover
        return None
    return st_ace

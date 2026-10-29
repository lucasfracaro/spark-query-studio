#!/usr/bin/env bash
# Abre o Spark Query Studio.
# Na primeira vez, quando o requirements.txt muda ou se a instalação anterior falhou no meio,
# (re)instala as dependências em .venv antes de abrir.
cd "$(dirname "$0")" || exit 1
MARCA=.venv/.instalado
if [ ! -f "$MARCA" ] || ! cmp -s requirements.txt "$MARCA"; then
  echo "Preparando o ambiente em .venv..."
  if [ ! -x .venv/bin/python ]; then
    python3 -m venv .venv || {
      echo "Não consegui criar o ambiente. Confira se o Python 3.10+ está instalado (no Ubuntu: sudo apt install python3-venv)."
      exit 1
    }
  fi
  .venv/bin/python -m pip install -r requirements.txt || {
    echo "A instalação das dependências falhou (veja o erro acima). Rode de novo: ela recomeça de onde parou."
    exit 1
  }
  cp requirements.txt "$MARCA"
fi
exec .venv/bin/python -m streamlit run app.py --server.address localhost --browser.gatherUsageStats false

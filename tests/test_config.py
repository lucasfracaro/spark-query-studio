"""Configurações de segurança do Streamlit e dos scripts de inicialização."""
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent


def _secoes(caminho: Path) -> dict[str, dict[str, str]]:
    """Leitor mínimo de TOML (só chave = valor dentro de [seções]); funciona no Python 3.10."""
    secoes, atual = {}, ""
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.split("#", 1)[0].strip()
        if linha.startswith("[") and linha.endswith("]"):
            atual = linha[1:-1].strip()
            secoes.setdefault(atual, {})
        elif "=" in linha:
            k, v = (p.strip() for p in linha.split("=", 1))
            secoes.setdefault(atual, {})[k] = v.strip('"')
    return secoes


def test_streamlit_so_aceita_conexoes_deste_computador():
    assert _secoes(RAIZ / ".streamlit" / "config.toml")["server"]["address"] == "localhost"


def test_telemetria_do_streamlit_desligada():
    assert _secoes(RAIZ / ".streamlit" / "config.toml")["browser"]["gatherUsageStats"] == "false"


def test_scripts_de_inicializacao_forcam_localhost():
    for script in ("run.sh", "run_windows.bat"):
        texto = (RAIZ / script).read_text(encoding="utf-8")
        assert "--server.address localhost" in texto, script
        assert "--browser.gatherUsageStats false" in texto, script

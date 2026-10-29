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


def _python_falso(pasta: Path, log: Path, falha_pip: Path) -> Path:
    """python3 que só registra o que pediram: cria .venv/bin/python e falha no pip se pedirem."""
    bin_ = pasta / "bin"
    bin_.mkdir()
    falso = bin_ / "python3"
    falso.write_text(f"""#!/usr/bin/env bash
echo "$*" >> "{log}"
if [ "$1" = "-m" ] && [ "$2" = "venv" ]; then
  mkdir -p "$3/bin" && cp "$0" "$3/bin/python" && chmod +x "$3/bin/python"; exit 0
fi
if [ "$1" = "-m" ] && [ "$2" = "pip" ] && [ -f "{falha_pip}" ]; then exit 1; fi
exit 0
""")
    falso.chmod(0o755)
    return bin_


def test_run_sh_reinstala_se_a_instalacao_falhou_ou_o_requirements_mudou(tmp_path):
    import os
    import shutil
    import subprocess
    import sys
    if sys.platform == "win32" or not shutil.which("bash") or not shutil.which("cmp"):
        import pytest
        pytest.skip("script de Linux/macOS")
    proj, log, falha_pip = tmp_path / "proj", tmp_path / "log.txt", tmp_path / "pip_falha"
    proj.mkdir()
    shutil.copy(RAIZ / "run.sh", proj)
    shutil.copy(RAIZ / "requirements.txt", proj)
    env = dict(os.environ, PATH=f"{_python_falso(tmp_path, log, falha_pip)}:{os.environ['PATH']}")

    def rodar():
        log.write_text("")
        r = subprocess.run(["bash", str(proj / "run.sh")], env=env, capture_output=True, text=True, timeout=60)
        return r.returncode, log.read_text().splitlines()

    falha_pip.touch()
    codigo, chamadas = rodar()                           # a instalação falha no meio
    assert codigo == 1 and not (proj / ".venv" / ".instalado").exists()
    falha_pip.unlink()
    codigo, chamadas = rodar()                           # a próxima vez reinstala (sem recriar o .venv)
    assert codigo == 0 and chamadas == ["-m pip install -r requirements.txt",
                                        "-m streamlit run app.py --server.address localhost "
                                        "--browser.gatherUsageStats false"]
    codigo, chamadas = rodar()                           # instalado: só abre
    assert chamadas == ["-m streamlit run app.py --server.address localhost --browser.gatherUsageStats false"]
    with open(proj / "requirements.txt", "a") as f:
        f.write("novo-pacote\n")
    codigo, chamadas = rodar()                           # requirements mudou: instala de novo
    assert chamadas[0] == "-m pip install -r requirements.txt"


def test_run_windows_bat_tem_finais_de_linha_do_windows():
    dados = (RAIZ / "run_windows.bat").read_bytes()
    assert dados.count(b"\r\n") == dados.count(b"\n") and b"goto erro_pip" in dados

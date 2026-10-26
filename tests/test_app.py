"""Testes da interface com o executor de testes do próprio Streamlit (sem navegador).
Pulados se o Streamlit não estiver instalado."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(Path(__file__).resolve().parent.parent / "app.py")


@pytest.fixture
def app(tmp_path, monkeypatch):
    import settings_store
    monkeypatch.setenv("SQS_PLAIN_EDITOR", "1")
    # não lê nem grava a configuração real do usuário
    monkeypatch.setattr(settings_store, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(settings_store, "CONFIG_FILE", tmp_path / "config.json")
    at = AppTest.from_file(APP, default_timeout=30)
    at.run()
    assert not at.exception, at.exception
    return at


def _botao(at, texto):
    return next(b for b in at.button if texto in b.label)


def test_tela_abre_sem_erros(app):
    assert any("Spark Query Studio" in m.value for m in app.markdown)
    assert app.text_area[0].value.startswith("SELECT")


def test_query_vazia_mostra_erro(app):
    app.text_area[0].set_value("   ")
    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("vazia" in e.value for e in app.error)


def test_executa_com_motor_falso(app, monkeypatch):
    import engines

    resultado = {"ok": True, "elapsed": 0.1, "log": ["view x <- y"],
                 "columns": [{"name": "id", "type": "int"}, {"name": "id", "type": "int"}],
                 "rows": [[1, 2], [3, 4]]}

    class MotorFalso:
        session_id = "local"

        def __init__(self, *a, **k):
            pass

        def session_status(self, *a):
            return "READY"

        def start(self, on_status=None):
            return "local"

        def run(self, code, on_status=None, **k):
            compile(code, "<gerado>", "exec")
            return dict(resultado)

    monkeypatch.setattr(engines, "LocalSparkEngine", MotorFalso)
    app.session_state["engine_kind"] = "local"
    downloads_antes = len(app.get("download_button"))
    app.text_area[0].set_value("SELECT 1 AS id, 2 AS id")
    _botao(app, "Executar").click().run()

    assert not app.exception, app.exception
    assert not app.error, [e.value for e in app.error]
    tabelas = [d.value for d in app.dataframe]
    resultado_df = next(df for df in tabelas if list(df.columns) == ["id", "id_2"])  # nomes repetidos ganham sufixo
    assert len(resultado_df) == 2
    assert len(app.get("download_button")) == downloads_antes + 1   # botão Exportar CSV


def test_limpar(app):
    _botao(app, "Limpar").click().run()
    assert app.text_area[0].value == ""


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
    # Exportar CSV + dicionário de dados em CSV e JSON (aba Metadados)
    assert len(app.get("download_button")) == downloads_antes + 3


def _motor_que_registra(monkeypatch, resultado):
    import engines
    chamadas = []

    class Motor:
        session_id = "local"

        def __init__(self, *a, **k):
            pass

        def session_status(self, *a):
            return "READY"

        def start(self, on_status=None):
            return "local"

        def run(self, code, on_status=None, **k):
            chamadas.append(code)
            return dict(resultado)

    monkeypatch.setattr(engines, "LocalSparkEngine", Motor)
    return chamadas


def test_bloqueia_base_fora_da_orquestracao(app, monkeypatch):
    chamadas = _motor_que_registra(monkeypatch, {"ok": True, "columns": [], "rows": [], "log": []})
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT * FROM minha_view JOIN db.nao_declarada x ON 1=1").run()
    assert any("db.nao_declarada" in c.value for c in app.caption)       # aviso já na digitação

    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("`db.nao_declarada`" in e.value and "não está na orquestração" in e.value for e in app.error)
    assert chamadas == []                                                # nada foi enviado ao cluster


def test_desligar_a_conferencia_libera_a_execucao(app, monkeypatch):
    chamadas = _motor_que_registra(monkeypatch, {"ok": True, "columns": [], "rows": [], "log": []})
    app.session_state["engine_kind"] = "local"
    next(c for c in app.checkbox if "orquestração" in c.label).uncheck().run()
    app.text_area[0].set_value("SELECT * FROM db.nao_declarada").run()
    _botao(app, "Executar").click().run()
    assert len(chamadas) == 1


def test_erro_de_tabela_do_spark_vira_mensagem_clara(app, monkeypatch):
    _motor_que_registra(monkeypatch, {
        "ok": False, "log": [], "traceback": "...",
        "error": "AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `db`.`sumida` cannot be found."})
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT * FROM minha_view").run()
    _botao(app, "Executar").click().run()
    assert any("`db.sumida`" in e.value and "orquestração" in e.value for e in app.error)


def test_botoes_de_orquestracao_e_athena(app):
    rotulos = [b.label for b in app.button]
    assert any("Importar query do Athena" in r for r in rotulos)
    assert any("Abrir orquestração" in r for r in rotulos)


def test_limpar(app):
    _botao(app, "Limpar").click().run()
    assert app.text_area[0].value == ""



def test_janela_de_orquestracao_ve_edicoes_ainda_nao_aplicadas(app, monkeypatch):
    # Simula uma edição na tabela de entradas que ainda não foi "aplicada" em lugar nenhum:
    # a janela precisa mostrar (e salvar) a versão editada, não a anterior.
    import streamlit as st
    original = st.data_editor

    def editor_editado(data, *args, key=None, **kwargs):
        out = original(data, *args, key=key, **kwargs)
        if key and key.startswith("inputs_editor"):
            out = out.copy()
            out.loc[0, "alias"] = "alias_editado"
        return out

    monkeypatch.setattr(st, "data_editor", editor_editado)
    _botao(app, "Abrir orquestração").click().run()
    assert not app.exception, app.exception
    janela = next(t for t in app.text_area if t.label == "JSON da orquestração")
    assert '"alias_editado"' in janela.value


def test_modo_somente_leitura_bloqueia_comando_que_grava(app, monkeypatch):
    chamadas = _motor_que_registra(monkeypatch, {"ok": True, "columns": [], "rows": [], "log": []})
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("INSERT INTO db.destino SELECT * FROM minha_view").run()
    assert any("somente leitura" in c.value for c in app.caption)
    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("grava ou altera dados" in e.value for e in app.error)
    assert chamadas == []


def test_liberar_escrita_permite_executar(app, monkeypatch):
    chamadas = _motor_que_registra(monkeypatch, {"ok": True, "columns": [], "rows": [], "log": []})
    app.session_state["engine_kind"] = "local"
    next(c for c in app.checkbox if "gravam ou alteram" in c.label).check().run()
    app.text_area[0].set_value("INSERT INTO db.destino SELECT * FROM minha_view").run()
    _botao(app, "Executar").click().run()
    assert len(chamadas) == 1


def test_perfil_aws_inexistente_nao_derruba_a_tela(app):
    from engines import AwsAccount
    app.session_state["account"] = AwsAccount(region="sa-east-1", auth_mode="profile",
                                              profile="perfil_que_nao_existe_sqs")
    app.session_state["engine_kind"] = "glue"
    app.run()
    assert not app.exception, app.exception
    assert any("Configurar conta AWS" in e.value for e in app.error)

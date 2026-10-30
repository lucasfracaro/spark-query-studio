"""Testes da interface com o executor de testes do próprio Streamlit (sem navegador).
Pulados se o Streamlit não estiver instalado."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(Path(__file__).resolve().parent.parent / "app.py")


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    """Arquivo de configuração só deste teste (a configuração real do usuário nunca é lida)."""
    import settings_store
    arquivo = tmp_path / "configuracao.toml"
    monkeypatch.setenv(settings_store.ENV_VAR, str(arquivo))
    monkeypatch.setattr(settings_store, "LEGACY_FILE", tmp_path / "legado.json")
    return arquivo


@pytest.fixture
def novo_app(config_file, tmp_path, monkeypatch):
    import session_hub
    monkeypatch.setenv("SQS_PLAIN_EDITOR", "1")
    # sem credenciais AWS de verdade: as chamadas falham na hora, em vez de tentar a rede
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
                "AWS_DEFAULT_PROFILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws_config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws_credentials"))
    session_hub.HUB.reset()

    def criar():
        at = AppTest.from_file(APP, default_timeout=30)
        at.run()
        assert not at.exception, at.exception
        return at
    return criar


@pytest.fixture
def app(novo_app):
    return novo_app()


def _botao(at, texto):
    return next(b for b in at.button if texto in b.label)


def _motor_falso(monkeypatch, resultado=None, chamadas=None):
    """Troca o Spark local por um motor que só registra o código recebido."""
    import engines
    resultado = resultado or {"ok": True, "columns": [], "rows": [], "log": []}
    chamadas = chamadas if chamadas is not None else []

    class Motor:
        kind = "local"
        session_id = "local"

        def __init__(self, instance=None, account=None):
            self.instance = instance

        def session_status(self, *a):
            return "READY"

        def ensure_session(self, on_status=None, on_created=None):
            return "local"

        def cancel_statement(self, *a, **k):
            return False

        def stop(self, *a):
            pass

        def run(self, code, on_status=None, **k):
            compile(code, "<gerado>", "exec")
            chamadas.append(code)
            return dict(resultado)

    monkeypatch.setattr(engines, "LocalSparkEngine", Motor)
    return chamadas


# --------------------------------------------------------------------------- tela e execução
def test_tela_abre_sem_erros(app):
    assert any("Spark Query Studio" in m.value for m in app.markdown)
    assert app.text_area[0].value.startswith("SELECT")


def test_query_vazia_mostra_erro(app):
    app.text_area[0].set_value("   ")
    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("vazia" in e.value for e in app.error)


def test_executa_com_motor_falso(app, monkeypatch):
    _motor_falso(monkeypatch, {"ok": True, "elapsed": 0.1, "log": ["view x <- y"],
                               "columns": [{"name": "id", "type": "int"}, {"name": "id", "type": "int"}],
                               "rows": [[1, 2], [3, 4]]})
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
    assert any(s.label == "Formato do CSV" for s in app.selectbox)


def test_bloqueia_base_fora_da_orquestracao(app, monkeypatch):
    chamadas = _motor_falso(monkeypatch)
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT * FROM minha_view JOIN db.nao_declarada x ON 1=1").run()
    assert any("db.nao_declarada" in c.value for c in app.caption)       # aviso já na digitação

    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("`db.nao_declarada`" in e.value and "não está na orquestração" in e.value for e in app.error)
    assert chamadas == []                                                # nada foi enviado ao cluster


def test_desligar_a_conferencia_libera_a_execucao(app, monkeypatch):
    chamadas = _motor_falso(monkeypatch)
    app.session_state["engine_kind"] = "local"
    next(c for c in app.checkbox if "orquestração" in c.label).uncheck().run()
    app.text_area[0].set_value("SELECT * FROM db.nao_declarada").run()
    _botao(app, "Executar").click().run()
    assert len(chamadas) == 1


def test_erro_de_tabela_do_spark_vira_mensagem_clara(app, monkeypatch):
    _motor_falso(monkeypatch, {"ok": False, "log": [], "traceback": "...",
                               "error": "AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
                                        "`db`.`sumida` cannot be found."})
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT * FROM minha_view").run()
    _botao(app, "Executar").click().run()
    assert any("`db.sumida`" in e.value and "orquestração" in e.value for e in app.error)


def test_erro_do_spark_vem_resumido_com_dica(app, monkeypatch):
    erro = ("Py4JJavaError: An error occurred while calling o120.showString.\n"
            ": java.nio.file.AccessDeniedException: s3://bucket-da-outra-conta/vendas/part-0.parquet: "
            "Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied)\n"
            "\tat org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:255)\n")
    _motor_falso(monkeypatch, {"ok": False, "log": [], "traceback": "Traceback ...", "error": erro})
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT * FROM minha_view").run()
    _botao(app, "Executar").click().run()
    assert any("Acesso negado ao S3" in e.value for e in app.error)
    assert any("Lake Formation" in i.value for i in app.info)
    resumo = [c.value for c in app.code]
    assert any("AccessDeniedException" in c and "S3AUtils" not in c for c in resumo)     # sem a pilha do Java


def test_botoes_de_orquestracao_e_athena(app):
    rotulos = [b.label for b in app.button]
    assert any("Importar query do Athena" in r for r in rotulos)
    assert any("Abrir orquestração" in r for r in rotulos)


def test_limpar(app):
    _botao(app, "Limpar").click().run()
    assert app.text_area[0].value == ""


def test_modo_somente_leitura_bloqueia_comando_que_grava(app, monkeypatch):
    chamadas = _motor_falso(monkeypatch)
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("INSERT INTO db.destino SELECT * FROM minha_view").run()
    assert any("somente leitura" in c.value for c in app.caption)
    _botao(app, "Executar").click().run()
    assert not app.exception
    assert any("grava ou altera dados" in e.value for e in app.error)
    assert chamadas == []


def test_liberar_escrita_permite_executar(app, monkeypatch):
    chamadas = _motor_falso(monkeypatch)
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
    assert any("Configuração" in e.value for e in app.error)


def test_glue_6_avisa_do_modo_ansi(app):
    app.session_state["engine_kind"] = "glue"
    app.run()
    next(s for s in app.selectbox if s.label == "Versão do Glue").set_value("6.0").run()
    assert any("ANSI" in c.value for c in app.caption)


# --------------------------------------------------------------------------- sessão
def _slot_glue_padrao():
    import engines
    import session_hub
    motor = engines.GlueSessionEngine(engines.AwsAccount(), engines.InstanceConfig(), glue_client=object(),
                                      sts_client=object())
    return session_hub.HUB.slot(motor)


def test_oferece_reaproveitar_sessao_anterior(app, monkeypatch):
    import engines
    monkeypatch.setattr(engines.GlueSessionEngine, "session_status", lambda self, *a: "READY")
    slot = _slot_glue_padrao()
    slot.reusable = [{"Id": "sqs-abc", "Status": "READY", "fingerprint": "8 x G.2X · Glue 4.0", "IdleTimeout": 30}]
    app.session_state["engine_kind"] = "glue"
    app.run()
    assert any("sqs-abc" in i.value for i in app.info)
    _botao(app, "Reaproveitar").click().run()
    assert not app.exception, app.exception
    assert slot.session_id == "sqs-abc"
    assert any("sqs-abc" in s.value for s in app.success)
    # a sessão foi criada com outra configuração: avisa que precisa encerrar para aplicar a atual
    assert any("8 x G.2X" in w.value for w in app.warning)


def test_sessao_do_app_vale_para_outra_aba(novo_app, monkeypatch):
    import engines
    monkeypatch.setattr(engines.GlueSessionEngine, "session_status", lambda self, *a: "READY")
    slot = _slot_glue_padrao()
    slot.session_id, slot.status, slot.fingerprint = "sqs-da-outra-aba", "READY", "2 x G.1X · Glue 5.0"
    at = novo_app()
    at.session_state["engine_kind"] = "glue"
    at.run()
    assert any("sqs-da-outra-aba" in s.value for s in at.success)
    assert any("Encerra sozinha" in c.value for c in at.caption) or slot.expected_end() is None


def test_limpar_cache_da_sessao(app, monkeypatch):
    chamadas = _motor_falso(monkeypatch)
    app.session_state["engine_kind"] = "local"
    app.run()
    _botao(app, "Limpar cache").click().run()
    assert not app.exception, app.exception
    assert len(chamadas) == 1 and "clearCache" in chamadas[0]


# --------------------------------------------------------------------------- Lake Formation (Glue 4.0)
def _glue_4_com_lake_formation(app):
    app.session_state["engine_kind"] = "glue"
    app.run()
    next(s for s in app.selectbox if s.label == "Versão do Glue").set_value("4.0").run()
    next(c for c in app.checkbox if c.label == "Ler as tabelas pelo Lake Formation").check().run()


def test_lake_formation_no_glue_4_le_pelo_glue_context(app):
    _glue_4_com_lake_formation(app)
    assert not app.exception, app.exception
    assert any("GlueContext" in c.value for c in app.caption)
    codigo = next(c.value for c in app.code if "_statements" in c.value)
    assert "'reader': 'glue_context'" in codigo and "create_dynamic_frame.from_catalog" in codigo


def test_lake_formation_no_glue_4_exige_o_alias(app):
    _glue_4_com_lake_formation(app)
    app.text_area[0].set_value("SELECT * FROM meu_database.minha_tabela").run()
    assert any("Use o alias" in c.value for c in app.caption)
    _botao(app, "Executar").click().run()
    assert any("use o alias `minha_view`" in e.value for e in app.error)


# --------------------------------------------------------------------------- arquivo de configuração
def test_configuracao_vem_do_arquivo(config_file, novo_app):
    config_file.write_text('[aws]\nregiao = "sa-east-1"\nrole_glue = "arn:aws:iam::123456789012:role/Glue"\n'
                           '[instancia]\nworkers = 7\nversao_glue = "4.0"\n', encoding="utf-8")
    at = novo_app()
    assert at.session_state["account"].region == "sa-east-1"
    assert at.session_state["instance"].num_workers == 7
    assert any("Configuração lida de `configuracao.toml`" in c.value for c in at.caption)
    assert not any("precisa de ajuste" in w.value for w in at.warning)


def test_configuracao_com_problema_mostra_aviso_e_abre_a_janela(config_file, novo_app):
    config_file.write_text('[aws]\nregiao = "sa-east1"\nperfil = dev\n', encoding="utf-8")    # erro de digitação
    at = novo_app()
    assert any("precisa de ajuste" in w.value for w in at.warning)
    _botao(at, "Abrir a configuração para corrigir").click().run()
    assert not at.exception, at.exception
    texto = next(t for t in at.text_area if t.label == "Conteúdo do configuracao.toml")
    assert "perfil = dev" in texto.value
    assert any("Erro de digitação" in e.value for e in at.error)


# O AppTest não simula as janelas (st.dialog): cada interação roda a página inteira e a janela some.
# Por isso o conteúdo das janelas é testado desenhado direto na página.
def _script_janela(nome):
    from ui import state
    state.init()
    if nome == "config":
        from ui.config_dialog import config_body
        config_body()
    elif nome == "orquestracao":
        from ui.orchestration import orchestration_body
        orchestration_body()
    else:
        from ui.orchestration import athena_body
        athena_body()


@pytest.fixture
def janela(novo_app):          # novo_app: mesmo isolamento de configuração e de AWS da tela inteira
    def abrir(nome, **estado):
        at = AppTest.from_function(_script_janela, args=(nome,), default_timeout=30)
        for chave, valor in estado.items():
            at.session_state[chave] = valor
        at.run()
        assert not at.exception, at.exception
        return at
    return abrir


def test_janela_de_configuracao_corrige_o_texto_do_arquivo(config_file, janela):
    config_file.write_text('[aws]\nregiao = "sa-east-1"\nperfil = dev\n', encoding="utf-8")
    at = janela("config")
    texto = next(t for t in at.text_area if t.label == "Conteúdo do configuracao.toml")
    assert _botao(at, "Salvar este texto no arquivo").disabled            # com erro, não grava
    texto.set_value('[aws]\nregiao = "sa-east-1"\nperfil = "dev"\n').run()
    assert any("não tem erros" in c.value for c in at.caption)
    _botao(at, "Salvar este texto no arquivo").click().run()
    assert not at.exception, at.exception
    assert config_file.read_text(encoding="utf-8") == '[aws]\nregiao = "sa-east-1"\nperfil = "dev"\n'
    assert at.session_state["config_loaded"].ok and at.session_state["account"].profile == "dev"


def test_janela_de_configuracao_salva_no_arquivo(config_file, janela):
    at = janela("config")
    next(t for t in at.text_input if t.label == "Região").set_value("sa-east-1").run()
    next(t for t in at.text_input if t.label == "ARN da IAM Role do Glue").set_value(
        "arn:aws:iam::123456789012:role/GlueDev").run()
    next(n for n in at.number_input if n.label == "Número de workers").set_value(6).run()
    next(c for c in at.checkbox if "Lake Formation" in c.label).check().run()
    assert any("GlueContext" in c.value or "Full Table Access" in c.value for c in at.caption)
    _botao(at, "Salvar no arquivo").click().run()
    assert not at.exception, at.exception
    texto = config_file.read_text(encoding="utf-8")
    assert 'regiao = "sa-east-1"' in texto and "workers = 6" in texto and "GlueDev" in texto
    assert "lake_formation = true" in texto
    assert at.session_state["instance"].num_workers == 6 and at.session_state["instance"].lake_formation


def test_janela_de_configuracao_aplicar_nao_mexe_no_arquivo(config_file, janela):
    at = janela("config")
    next(t for t in at.text_input if t.label == "Região").set_value("eu-west-1").run()
    _botao(at, "Aplicar").click().run()
    assert at.session_state["account"].region == "eu-west-1" and not config_file.exists()


def test_janela_de_configuracao_aponta_valor_invalido(janela):
    at = janela("config")
    next(t for t in at.text_input if t.label == "Região").set_value("brasil").run()
    assert any("Região `brasil` inválida" in w.value for w in at.warning)
    assert _botao(at, "Salvar no arquivo").disabled


def test_chaves_nao_vao_para_o_arquivo_sem_pedir(config_file, janela):
    at = janela("config")
    next(s for s in at.selectbox if s.label == "Autenticação").set_value("keys").run()
    next(t for t in at.text_input if t.label == "Access key ID").set_value("AKIAEXEMPLO").run()
    next(t for t in at.text_input if t.label == "Secret access key").set_value("segredo").run()
    _botao(at, "Salvar no arquivo").click().run()
    assert not at.exception, at.exception
    texto = config_file.read_text(encoding="utf-8")
    assert "segredo" not in texto and 'autenticacao = "chaves"' in texto
    assert at.session_state["account"].secret_key == "segredo"          # continua valendo enquanto o app roda


# --------------------------------------------------------------------------- janelas de orquestração e Athena
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


def test_janela_de_orquestracao_aplica_o_json(janela):
    at = janela("orquestracao", orch_draft='{"entradas": []}')
    texto = next(t for t in at.text_area if t.label == "JSON da orquestração")
    texto.set_value('{"entradas": [{"alias": "vendas", "source": "db.vendas", "filter": "dt = \'1\'"}], '
                    '"database_padrao": "db"}').run()
    assert any("1 entrada(s)" in c.value for c in at.caption)
    _botao(at, "Aplicar").click().run()
    assert not at.exception, at.exception
    entradas = at.session_state["inputs_base"]
    assert list(entradas["alias"]) == ["vendas"] and list(entradas["filter"]) == ["dt = '1'"]


def test_janela_de_orquestracao_mostra_json_invalido(janela):
    at = janela("orquestracao", orch_draft='{"entradas": []}')
    next(t for t in at.text_area if t.label == "JSON da orquestração").set_value("{ isso não é json").run()
    assert at.error and _botao(at, "Aplicar").disabled


def test_janela_do_athena_monta_a_orquestracao(janela):
    pytest.importorskip("sqlglot")
    at = janela("athena", engine_kind="local")          # sem consultar o Glue
    sql = ("WITH v AS (SELECT * FROM db.vendas WHERE dt = '2026-10-01')\n"
           "SELECT cliente, sum(valor) AS total FROM v GROUP BY 1")
    next(t for t in at.text_area if t.label == "Query do Athena").set_value(sql).run()
    _botao(at, "Gerar orquestração").click().run()
    assert not at.exception, at.exception
    assert any("db.vendas" in str(df.value.values) for df in at.dataframe)
    _botao(at, "Aplicar e revisar a orquestração").click().run()
    assert not at.exception, at.exception
    entradas = at.session_state["inputs_base"]
    assert list(entradas["source"]) == ["db.vendas"] and "dt" in entradas["filter"][0]
    assert "db.vendas" not in at.session_state["sql"]           # a query passa a usar o alias
    assert at.session_state["open_orch_after"] is True          # a orquestração abre para revisar


def _aws_falsa(monkeypatch, **clientes):
    """Troca os clientes boto3 da conta por falsos (glue, s3, athena)."""
    import engines
    monkeypatch.setattr(engines.AwsAccount, "client", lambda self, servico: clientes[servico])


def test_janela_do_athena_estima_o_tamanho_da_sessao(janela, monkeypatch):
    pytest.importorskip("sqlglot")
    from tests.fakes import FakeAthena, FakeCatalog, FakeS3, athena_execution, glue_partition, glue_table
    gib = 1024 ** 3
    sql = "SELECT cliente, valor FROM db.vendas WHERE dt BETWEEN '2026-10-01' AND '2026-10-03'"
    particoes = [glue_partition(f"2026-10-0{d}", totalSize=40 * gib) for d in range(1, 6)]
    athena = FakeAthena([athena_execution("a1", sql, scanned=60 * gib)], runtime={"a1": {
        "Rows": {"InputRows": 10 ** 9}, "OutputStage": {"SubStages": [{"OutputBytes": 200 * gib}]}}})
    _aws_falsa(monkeypatch, glue=FakeCatalog({("db", "vendas"): glue_table()}, {("db", "vendas"): particoes}),
               s3=FakeS3(), athena=athena)
    at = janela("athena", engine_kind="glue")
    next(t for t in at.text_area if t.label == "Query do Athena").set_value(sql).run()
    _botao(at, "Gerar orquestração").click().run()
    assert not at.exception, at.exception

    est = at.session_state["sizing"]
    assert est.ok and est.confidence == "alta" and est.read_bytes == 60 * gib
    assert any(est.label in s.value and "DPU" in s.value for s in at.success)
    assert any("Athena, mesma query" in c.value for c in at.caption)
    assert any("vendas" in str(df.value.values) and "120 GB" in str(df.value.values) for df in at.dataframe)
    # o tempo desejado refaz a conta na hora, sem consultar a AWS de novo
    chamadas = len(athena.calls)
    next(s for s in at.selectbox if s.label == "Processar em").set_value(600).run()
    assert at.session_state["sizing"].num_workers < est.num_workers and len(athena.calls) == chamadas
    est = at.session_state["sizing"]

    _botao(at, "Aplicar e revisar a orquestração").click().run()
    assert not at.exception, at.exception
    inst = at.session_state["instance"]
    assert (inst.worker_type, inst.num_workers) == (est.worker_type, est.num_workers)
    conf = dict(zip(at.session_state["conf_base"]["chave"], at.session_state["conf_base"]["valor"], strict=True))
    assert conf["spark.sql.shuffle.partitions"] == str(est.shuffle_partitions)
    assert conf["spark.sql.adaptive.enabled"] == "true"              # as outras configurações continuam


def test_janela_do_athena_sem_estimativa_nao_consulta_a_aws(janela, monkeypatch):
    pytest.importorskip("sqlglot")
    _aws_falsa(monkeypatch)                                           # qualquer cliente pedido daria erro
    at = janela("athena", engine_kind="local")
    next(t for t in at.text_area if t.label == "Query do Athena").set_value("SELECT * FROM db.t").run()
    _botao(at, "Gerar orquestração").click().run()
    assert not at.exception, at.exception
    assert at.session_state["sizing_data"] is None
    assert not any("Tamanho sugerido" in m.value for m in at.markdown)


def test_barra_lateral_sugere_a_ultima_estimativa(novo_app):
    import sizing
    at = AppTest.from_file(APP, default_timeout=30)
    at.session_state["sizing"] = sizing.Estimate(worker_type="G.2X", num_workers=9, confidence="média")
    at.run()
    assert not at.exception, at.exception
    assert any("**G.2X × 9**" in c.value for c in at.sidebar.caption)
    next(b for b in at.sidebar.button if b.label == "Usar").click().run()
    assert not at.exception, at.exception
    inst = at.session_state["instance"]
    assert (inst.worker_type, inst.num_workers) == ("G.2X", 9)
    assert not any("**G.2X × 9**" in c.value for c in at.sidebar.caption)     # já está em uso


# --------------------------------------------------------------------------- database padrão
def _campo_db(at):
    return next(t for t in at.text_input if t.label == "Database padrão (USE)")


def test_database_padrao_vem_preenchido_e_pode_ficar_vazio(config_file, novo_app):
    at = novo_app()
    assert _campo_db(at).value == "workspace_db" and at.session_state["default_db"] == "workspace_db"
    config_file.write_text('[instancia]\ndatabase_padrao = ""\n', encoding="utf-8")
    at = novo_app()
    assert _campo_db(at).value == "" and at.session_state["default_db"] == ""


def test_database_padrao_da_configuracao_vale_a_nao_ser_que_tenha_sido_trocado(janela):
    at = janela("config")
    _campo_db(at).set_value("analytics_db").run()
    _botao(at, "Aplicar").click().run()
    assert at.session_state["default_db"] == "analytics_db"            # seguiu a configuração
    at.session_state["default_db"] = "meu_db"                          # trocado na orquestração
    at.run()
    _campo_db(at).set_value("outro_db").run()
    _botao(at, "Aplicar").click().run()
    assert at.session_state["default_db"] == "meu_db"                   # a escolha da orquestração fica


def test_aviso_do_log_aparece_no_resultado(app, monkeypatch):
    aviso = "AVISO: o database padrão workspace_db não foi usado (Database not found). A query segue sem ele."
    _motor_falso(monkeypatch, {"ok": True, "elapsed": 0.1, "log": ["USE x", aviso],
                               "columns": [{"name": "id", "type": "int"}], "rows": [[1]]})
    app.session_state["engine_kind"] = "local"
    _botao(app, "Executar").click().run()
    assert not app.exception, app.exception
    assert any(c.value.startswith("⚠️ o database padrão workspace_db não foi usado") for c in app.caption)


# --------------------------------------------------------------------------- resultado completo
def test_preparar_e_baixar_o_csv_completo(app, monkeypatch):
    import ui.results
    _motor_falso(monkeypatch, {"ok": True, "elapsed": 0.1, "log": [],
                               "columns": [{"name": "id", "type": "int"}], "rows": [[1], [2]]})
    pedidos = []

    def completo(run, sql, orch, reader, formato, on_status=None, **k):
        pedidos.append((sql, orch.default_database, reader, formato))
        return "﻿id\n1\n2\n3\n".encode(), {"rows": 3, "truncated": False, "bytes": 30, "chunks": 1}

    monkeypatch.setattr(ui.results, "fetch_full_result", completo)
    app.session_state["engine_kind"] = "local"
    app.text_area[0].set_value("SELECT id FROM minha_view").run()
    _botao(app, "Executar").click().run()
    assert not app.exception, app.exception
    assert not any("Baixar CSV completo" in b.label for b in app.get("download_button"))
    _botao(app, "Preparar CSV com todas as linhas").click().run()
    assert not app.exception, app.exception
    assert pedidos == [("SELECT id FROM minha_view", "workspace_db", "spark", "excel_br")]   # a mesma execução
    assert any("Baixar CSV completo (3 linhas" in b.label for b in app.get("download_button"))
    _botao(app, "Executar").click().run()                    # resultado novo: o arquivo antigo some
    assert not any("Baixar CSV completo" in b.label for b in app.get("download_button"))

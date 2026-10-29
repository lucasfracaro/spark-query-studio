import base64
import json

import pytest

import engines
from code_builder import RESULT_MARKER
from engines import AwsAccount, EngineError, GlueSessionEngine, InstanceConfig
from fakes import FakeGlueClient, FakeSts


@pytest.fixture(autouse=True)
def sem_espera(monkeypatch):
    monkeypatch.setattr(GlueSessionEngine, "POLL_S", 0)
    monkeypatch.setattr(engines.time, "sleep", lambda s: None)


def _conta(**kw):
    base = dict(region="sa-east-1", glue_role_arn="arn:aws:iam::123456789012:role/Glue",
                temp_dir="s3://bucket/tmp/")
    base.update(kw)
    return AwsAccount(**base)


def _saida(obj):
    return {"Status": "OK", "Data": {"TextPlain": "log\n" + RESULT_MARKER
                                     + base64.b64encode(json.dumps(obj).encode()).decode()}}


# --------------------------------------------------------------------------- sessão
def test_start_cria_sessao_com_a_instancia_escolhida():
    glue = FakeGlueClient(session_states=["PROVISIONING", "PROVISIONING", "READY"])
    inst = InstanceConfig(worker_type="G.2X", num_workers=5, glue_version="4.0", idle_timeout_min=15,
                          extra_args={"--datalake-formats": "iceberg"}, tags={"projeto": "sqs"})
    status = []
    sid = GlueSessionEngine(_conta(), inst, glue_client=glue, sts_client=FakeSts()).start(status.append)

    params = glue.created[0]
    assert sid == params["Id"] and sid.startswith("sqs-")
    assert params["WorkerType"] == "G.2X" and params["NumberOfWorkers"] == 5
    assert params["GlueVersion"] == "4.0" and params["IdleTimeout"] == 15
    assert params["Role"].endswith("role/Glue")
    assert params["Command"] == {"Name": "glueetl", "PythonVersion": "3"}
    assert params["DefaultArguments"]["--enable-glue-datacatalog"] == "true"
    assert params["DefaultArguments"]["--TempDir"] == "s3://bucket/tmp/"
    assert params["DefaultArguments"]["--datalake-formats"] == "iceberg"
    assert params["Tags"] == {"projeto": "sqs", engines.OWNER_TAG: FakeSts().arn}
    assert any("READY" in s for s in status)


def test_start_sem_role():
    with pytest.raises(EngineError, match="IAM Role"):
        GlueSessionEngine(_conta(glue_role_arn=""), InstanceConfig(), glue_client=FakeGlueClient(), sts_client=FakeSts()).start()


def test_start_tolera_sessao_ainda_nao_visivel():
    glue = FakeGlueClient(session_states=[None, None, "PROVISIONING", "READY"])
    assert GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts()).start()


@pytest.mark.parametrize("estado", ["FAILED", "TIMEOUT", "STOPPED"])
def test_start_falha(estado):
    glue = FakeGlueClient(session_states=["PROVISIONING", estado])
    with pytest.raises(EngineError, match=estado):
        GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts()).start()


def test_start_sessao_some():
    glue = FakeGlueClient(session_states=[None])
    with pytest.raises(EngineError):
        GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts()).start()


def test_attach_reaproveita_sessao_pronta():
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=FakeGlueClient(session_states=["READY"]), sts_client=FakeSts())
    assert eng.attach("sqs-123") is True and eng.session_id == "sqs-123"


def test_attach_recusa_sessao_encerrada():
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=FakeGlueClient(session_states=["TIMEOUT"]), sts_client=FakeSts())
    assert eng.attach("sqs-123") is False and eng.session_id is None


def test_session_status_propaga_outros_erros():
    class Quebrado(FakeGlueClient):
        def get_session(self, Id):
            raise PermissionError("AccessDenied")
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=Quebrado(), sts_client=FakeSts())
    eng.session_id = "x"
    with pytest.raises(PermissionError):
        eng.session_status()


def test_stop():
    glue = FakeGlueClient()
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts())
    eng.session_id = "sqs-1"
    eng.stop()
    assert glue.stopped == ["sqs-1"] and eng.session_id is None


# --------------------------------------------------------------------------- execução
def _engine_pronto(glue):
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts())
    eng.session_id = "sqs-1"
    return eng


def test_run_devolve_resultado():
    esperado = {"ok": True, "columns": [{"name": "a", "type": "int"}], "rows": [[1]], "log": []}
    glue = FakeGlueClient(statement_states=["WAITING", "RUNNING", "AVAILABLE"], output=_saida(esperado))
    status = []
    assert _engine_pronto(glue).run("print(1)", status.append) == esperado
    assert glue.statements == ["print(1)"]
    assert any("50%" in s for s in status)          # progresso repassado para a tela


def test_run_cria_sessao_se_preciso():
    glue = FakeGlueClient(session_states=["READY"], output=_saida({"ok": True}))
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts())
    assert eng.run("x")["ok"] is True
    assert len(glue.created) == 1


def test_run_erro_python_na_sessao():
    out = {"Status": "ERROR", "ErrorName": "NameError", "ErrorValue": "name 'x' is not defined",
           "Traceback": ["linha 1", "linha 2"]}
    res = _engine_pronto(FakeGlueClient(output=out)).run("x")
    assert res["ok"] is False
    assert res["error"] == "NameError: name 'x' is not defined"
    assert res["traceback"] == "linha 1\nlinha 2"


def test_run_saida_sem_marcacao():
    res = _engine_pronto(FakeGlueClient(output={"Status": "OK", "Data": {"TextPlain": "nada"}})).run("x")
    assert res["ok"] is False and "nada" in res["traceback"]


@pytest.mark.parametrize("estado", ["ERROR", "CANCELLED"])
def test_run_statement_interrompido(estado):
    res = _engine_pronto(FakeGlueClient(statement_states=["RUNNING", estado])).run("x")
    assert res["ok"] is False and estado in res["error"]


def test_run_cancelamento():
    glue = FakeGlueClient(statement_states=["RUNNING", "RUNNING", "CANCELLED"])
    res = _engine_pronto(glue).run("x", should_cancel=lambda: True)
    assert glue.cancelled and res["ok"] is False


# --------------------------------------------------------------------------- conta
def test_boto_session_por_modo(monkeypatch):
    chamadas = []

    class FakeBoto3:
        @staticmethod
        def Session(**kw):
            chamadas.append(kw)
            return kw

    monkeypatch.setitem(__import__("sys").modules, "boto3", FakeBoto3)
    AwsAccount(region="us-east-1", auth_mode="profile", profile="dev").boto_session()
    AwsAccount(region="us-east-1", auth_mode="keys", access_key="AK", secret_key="SK").boto_session()
    AwsAccount(region="us-east-1", auth_mode="default").boto_session()
    AwsAccount(region="us-east-1", auth_mode="keys").boto_session()        # sem chaves: cai na cadeia padrão

    assert chamadas[0] == {"profile_name": "dev", "region_name": "us-east-1"}
    assert chamadas[1]["aws_access_key_id"] == "AK" and chamadas[1]["aws_session_token"] is None
    assert chamadas[2] == chamadas[3] == {"region_name": "us-east-1"}


# --------------------------------------------------------------------------- interrupções e sessões órfãs
class _Interrompido(BaseException):
    """Imita o que o Streamlit faz ao reiniciar o script no meio de uma espera."""


def test_id_da_sessao_e_entregue_antes_da_espera():
    class Glue(FakeGlueClient):
        def get_session(self, Id):
            raise _Interrompido()

    guardado = []
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=Glue(), sts_client=FakeSts())
    with pytest.raises(_Interrompido):
        eng.start(on_created=guardado.append)
    assert guardado == [eng.session_id] and guardado[0].startswith("sqs-")


def test_ensure_session_reaproveita_sessao_pronta():
    glue = FakeGlueClient(session_states=["READY"])
    eng = _engine_pronto(glue)
    assert eng.ensure_session() == "sqs-1"
    assert glue.created == []


def test_ensure_session_espera_sessao_que_ainda_esta_subindo():
    glue = FakeGlueClient(session_states=["PROVISIONING", "PROVISIONING", "READY"])
    eng = _engine_pronto(glue)
    status = []
    assert eng.ensure_session(status.append) == "sqs-1"
    assert glue.created == []                              # não criou outra
    assert any("ainda está iniciando" in s for s in status)


@pytest.mark.parametrize("estado", ["TIMEOUT", "STOPPED", None])
def test_ensure_session_cria_outra_se_a_anterior_acabou(estado):
    glue = FakeGlueClient(session_states=[estado, "READY"])
    eng = _engine_pronto(glue)
    criadas = []
    sid = eng.ensure_session(on_created=criadas.append)
    assert sid != "sqs-1" and criadas == [sid] and len(glue.created) == 1


def test_run_informa_o_id_do_statement():
    glue = FakeGlueClient(output=_saida({"ok": True}))
    enviados = []
    _engine_pronto(glue).run("x", on_submitted=enviados.append)
    assert enviados == [1]


def test_cancela_statement_que_ficou_rodando():
    glue = FakeGlueClient(statement_states=["RUNNING", "CANCELLING", "CANCELLED"])
    assert _engine_pronto(glue).cancel_statement(7) is True
    assert glue.cancelled == [7]


@pytest.mark.parametrize("estado", ["AVAILABLE", "ERROR", "CANCELLED"])
def test_nao_cancela_statement_que_ja_terminou(estado):
    glue = FakeGlueClient(statement_states=[estado])
    assert _engine_pronto(glue).cancel_statement(7) is False
    assert glue.cancelled == []


def test_stop_mostra_erro_de_permissao():
    class Glue(FakeGlueClient):
        def stop_session(self, Id):
            raise PermissionError("AccessDenied")
    eng = _engine_pronto(Glue())
    with pytest.raises(PermissionError):
        eng.stop()
    assert eng.session_id == "sqs-1"                       # continua sabendo da sessão


def test_stop_ignora_sessao_que_ja_nao_existe():
    from fakes import EntityNotFoundException

    class Glue(FakeGlueClient):
        def stop_session(self, Id):
            raise EntityNotFoundException(Id)
    eng = _engine_pronto(Glue())
    eng.stop()
    assert eng.session_id is None


def test_spark_ui_nao_e_ligado_sem_caminho_de_logs():
    glue = FakeGlueClient(session_states=["READY"])
    GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts()).start()
    assert "--enable-spark-ui" not in glue.created[0]["DefaultArguments"]



# --------------------------------------------------------------------------- reaproveitar sessão depois de recarregar a página
ARN = FakeSts().arn


def _sessao(sid, status="READY", dono=ARN, criada="2026-10-29T10:00:00"):
    return {"Id": sid, "Status": status, "CreatedOn": criada, "WorkerType": "G.1X", "NumberOfWorkers": 2,
            "GlueVersion": "5.0", "Tags": {engines.OWNER_TAG: dono}}


def test_sessoes_reaproveitaveis_so_as_minhas_ativas_e_desta_ferramenta():
    glue = FakeGlueClient(listed_sessions=[
        _sessao("sqs-antiga", criada="2026-10-29T08:00:00"),
        _sessao("sqs-nova", status="PROVISIONING", criada="2026-10-29T09:00:00"),
        _sessao("sqs-parada", status="STOPPED"),
        _sessao("sqs-do-colega", dono="arn:aws:sts::123456789012:assumed-role/Dev/outra-pessoa"),
        _sessao("notebook-do-glue"),
    ])
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts())
    assert [s["Id"] for s in eng.reusable_sessions()] == ["sqs-nova", "sqs-antiga"]
    assert glue.list_calls[0] == {engines.OWNER_TAG: ARN}


def test_sem_identidade_nao_procura_sessoes():
    glue = FakeGlueClient(listed_sessions=[_sessao("sqs-x")])
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts(falha=True))
    assert eng.reusable_sessions() == [] and glue.list_calls == []


def test_sem_identidade_a_sessao_e_criada_sem_a_tag_de_dono():
    glue = FakeGlueClient(session_states=["READY"])
    GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts(falha=True)).start()
    assert engines.OWNER_TAG not in glue.created[0].get("Tags", {})


def test_identidade_e_consultada_uma_vez_so():
    sts = FakeSts()
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=FakeGlueClient(), sts_client=sts)
    eng.caller_arn(), eng.caller_arn(), eng.reusable_sessions()
    assert sts.calls == 1


# --------------------------------------------------------------------------- versões e tipos de worker
def test_tipos_de_worker():
    assert "G.025X" not in engines.GLUE_WORKER_TYPES                  # só para jobs de streaming
    assert {"G.12X", "G.16X", "R.4X", "R.8X"} <= set(engines.GLUE_WORKER_TYPES)
    assert engines.GLUE_WORKER_TYPES["G.2X"].dpu == 2
    assert engines.DEFAULT_WORKER_TYPE in engines.GLUE_WORKER_TYPES
    assert "DPU" in engines.GLUE_WORKER_TYPES["R.1X"].descricao()


def test_versoes_do_glue():
    assert list(engines.GLUE_VERSIONS) == ["6.0", "5.1", "5.0", "4.0"]
    assert engines.DEFAULT_GLUE_VERSION in engines.GLUE_VERSIONS
    assert "ANSI" in engines.GLUE_VERSION_NOTES["6.0"]



def test_sem_permissao_de_tag_cria_a_sessao_sem_a_tag_de_dono():
    class AccessDeniedException(Exception):
        pass

    class Glue(FakeGlueClient):
        def create_session(self, **params):
            if engines.OWNER_TAG in params.get("Tags", {}):
                raise AccessDeniedException("User is not authorized to perform: glue:TagResource")
            return super().create_session(**params)

    glue = Glue(session_states=["READY"])
    GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue, sts_client=FakeSts()).start()
    assert len(glue.created) == 1 and "Tags" not in glue.created[0]


def test_outros_erros_de_permissao_continuam_aparecendo():
    class AccessDeniedException(Exception):
        pass

    class Glue(FakeGlueClient):
        def create_session(self, **params):
            raise AccessDeniedException("User is not authorized to perform: glue:CreateSession")

    with pytest.raises(AccessDeniedException):
        GlueSessionEngine(_conta(), InstanceConfig(), glue_client=Glue(), sts_client=FakeSts()).start()

import base64
import json

import pytest

import engines
from code_builder import RESULT_MARKER
from engines import AwsAccount, EngineError, GlueSessionEngine, InstanceConfig
from fakes import FakeGlueClient


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
    sid = GlueSessionEngine(_conta(), inst, glue_client=glue).start(status.append)

    params = glue.created[0]
    assert sid == params["Id"] and sid.startswith("sqs-")
    assert params["WorkerType"] == "G.2X" and params["NumberOfWorkers"] == 5
    assert params["GlueVersion"] == "4.0" and params["IdleTimeout"] == 15
    assert params["Role"].endswith("role/Glue")
    assert params["Command"] == {"Name": "glueetl", "PythonVersion": "3"}
    assert params["DefaultArguments"]["--enable-glue-datacatalog"] == "true"
    assert params["DefaultArguments"]["--TempDir"] == "s3://bucket/tmp/"
    assert params["DefaultArguments"]["--datalake-formats"] == "iceberg"
    assert params["Tags"] == {"projeto": "sqs"}
    assert any("READY" in s for s in status)


def test_start_sem_role():
    with pytest.raises(EngineError, match="IAM Role"):
        GlueSessionEngine(_conta(glue_role_arn=""), InstanceConfig(), glue_client=FakeGlueClient()).start()


def test_start_tolera_sessao_ainda_nao_visivel():
    glue = FakeGlueClient(session_states=[None, None, "PROVISIONING", "READY"])
    assert GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue).start()


@pytest.mark.parametrize("estado", ["FAILED", "TIMEOUT", "STOPPED"])
def test_start_falha(estado):
    glue = FakeGlueClient(session_states=["PROVISIONING", estado])
    with pytest.raises(EngineError, match=estado):
        GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue).start()


def test_start_sessao_some():
    glue = FakeGlueClient(session_states=[None])
    with pytest.raises(EngineError):
        GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue).start()


def test_attach_reaproveita_sessao_pronta():
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=FakeGlueClient(session_states=["READY"]))
    assert eng.attach("sqs-123") is True and eng.session_id == "sqs-123"


def test_attach_recusa_sessao_encerrada():
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=FakeGlueClient(session_states=["TIMEOUT"]))
    assert eng.attach("sqs-123") is False and eng.session_id is None


def test_session_status_propaga_outros_erros():
    class Quebrado(FakeGlueClient):
        def get_session(self, Id):
            raise PermissionError("AccessDenied")
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=Quebrado())
    eng.session_id = "x"
    with pytest.raises(PermissionError):
        eng.session_status()


def test_stop():
    glue = FakeGlueClient()
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue)
    eng.session_id = "sqs-1"
    eng.stop()
    assert glue.stopped == ["sqs-1"] and eng.session_id is None


# --------------------------------------------------------------------------- execução
def _engine_pronto(glue):
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue)
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
    eng = GlueSessionEngine(_conta(), InstanceConfig(), glue_client=glue)
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

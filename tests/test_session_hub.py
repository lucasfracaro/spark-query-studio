import base64
import json
import threading
import time

import pytest

import engines
import session_hub
from code_builder import RESULT_MARKER
from engines import AwsAccount, EngineError, GlueSessionEngine, InstanceConfig
from fakes import FakeGlueClient, FakeSts, IllegalSessionStateException, SessionBusyException

ARN = FakeSts().arn


@pytest.fixture(autouse=True)
def sem_espera(monkeypatch):
    monkeypatch.setattr(engines.time, "sleep", lambda s: None)
    monkeypatch.setattr(session_hub.time, "sleep", lambda s: None)


@pytest.fixture
def hub():
    return session_hub.SessionHub()


def _conta(**kw):
    return AwsAccount(**{"region": "sa-east-1", "glue_role_arn": "arn:aws:iam::123456789012:role/Glue", **kw})


def _motor(glue, inst=None, **conta):
    return GlueSessionEngine(_conta(**conta), inst or InstanceConfig(), glue_client=glue, sts_client=FakeSts())


def _saida(obj=None):
    obj = obj or {"ok": True, "columns": [], "rows": [], "log": []}
    return {"Status": "OK", "Data": {"TextPlain": RESULT_MARKER + base64.b64encode(json.dumps(obj).encode()).decode()}}


def _envelhece(hub, motor, segundos=60):
    """Faz de conta que o último status consultado é antigo."""
    hub.slot(motor).checked_at -= segundos


# --------------------------------------------------------------------------- uma sessão só
def test_cria_a_sessao_uma_vez_e_reaproveita(hub):
    glue = FakeGlueClient(session_states=["PROVISIONING", "READY"], output=_saida())
    motor = _motor(glue)
    assert hub.run(motor, "a")["ok"]
    chamadas = len(glue.get_session_calls)
    assert hub.run(_motor(glue), "b")["ok"]                 # outro motor (outra aba): mesma sessão
    assert len(glue.created) == 1
    assert len(glue.get_session_calls) == chamadas          # status conferido há pouco: sem nova consulta
    assert {sid for sid, _ in glue.statements} == {glue.created[0]["Id"]}


def test_confere_o_status_antes_de_rodar_se_passou_tempo(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    _envelhece(hub, motor)
    chamadas = len(glue.get_session_calls)
    hub.run(motor, "b")
    assert len(glue.get_session_calls) == chamadas + 1 and len(glue.created) == 1


def test_sessao_que_caiu_e_recriada_antes_de_rodar(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    _envelhece(hub, motor)
    glue.session_states = ["TIMEOUT", "READY"]              # encerrou por tempo sem uso
    avisos = []
    assert hub.run(motor, "b", avisos.append)["ok"]
    assert len(glue.created) == 2
    assert glue.statements[-1][0] == glue.created[1]["Id"]
    assert any("encerrou" in a for a in avisos)


def test_sessao_que_caiu_entre_a_conferencia_e_o_envio(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    glue.run_errors = [IllegalSessionStateException("Session is not ready")]
    assert hub.run(motor, "b")["ok"]                        # recusada no envio: cria outra e reenvia
    assert len(glue.created) == 2
    assert [c for _, c in glue.statements] == ["a", "b"]    # a query rodou uma vez só


def test_nao_reenvia_query_que_chegou_a_rodar(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida(),
                          get_statement_errors=[IllegalSessionStateException("Session stopped")])
    with pytest.raises(IllegalSessionStateException):
        hub.run(_motor(glue), "a")
    assert len(glue.statements) == 1 and len(glue.created) == 1


def test_sessao_ocupada_espera_e_tenta_de_novo(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida(),
                          run_errors=[SessionBusyException("busy"), SessionBusyException("busy")])
    avisos = []
    assert hub.run(_motor(glue), "a", avisos.append)["ok"]
    assert any("ocupada" in a for a in avisos)


def test_sessao_ocupada_por_muito_tempo_vira_erro_claro(hub, monkeypatch):
    glue = FakeGlueClient(session_states=["READY"], output=_saida(),
                          run_errors=[SessionBusyException("busy")] * 3)
    relogio = iter(range(0, 10_000, 40))
    monkeypatch.setattr(session_hub.time, "time", lambda: next(relogio))
    with pytest.raises(EngineError, match="ocupada"):
        hub.run(_motor(glue), "a")


def test_cancela_a_execucao_anterior_que_ficou_rodando(hub):
    glue = FakeGlueClient(session_states=["READY"], statement_states=["RUNNING", "AVAILABLE"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    slot = hub.slot(motor)
    slot.running = {"session": slot.session_id, "id": 7}
    glue.statement_states = ["RUNNING", "CANCELLED", "AVAILABLE"]
    avisos = []
    hub.run(motor, "b", avisos.append)
    assert 7 in glue.cancelled and any("anterior" in a for a in avisos)
    assert slot.running is None


def test_cancelar_pelo_botao(hub):
    glue = FakeGlueClient(session_states=["READY"], statement_states=["RUNNING", "CANCELLED"])
    motor = _motor(glue)
    slot = hub.slot(motor)
    slot.running = {"session": "sqs-x", "id": 3}
    assert hub.cancel_running(motor) is True
    assert glue.cancelled == [3] and slot.running is None
    assert hub.cancel_running(motor) is False


# --------------------------------------------------------------------------- sessão anterior (app reiniciado)
def _anterior(sid, inst, status="READY", dono=ARN):
    return {"Id": sid, "Status": status, "CreatedOn": "2026-10-29", "IdleTimeout": 30,
            "Description": f"Spark Query Studio · {inst.fingerprint()}", "Tags": {engines.OWNER_TAG: dono}}


def test_reaproveita_sessao_anterior_com_a_mesma_configuracao(hub):
    inst = InstanceConfig(num_workers=4)
    glue = FakeGlueClient(session_states=["READY"], output=_saida(),
                          listed_sessions=[_anterior("sqs-antiga", inst)])
    motor = _motor(glue, inst)
    avisos = []
    assert hub.run(motor, "a", avisos.append, fingerprint=inst.fingerprint())["ok"]
    assert glue.created == [] and glue.statements[0][0] == "sqs-antiga"
    assert hub.slot(motor).adopted == "sqs-antiga" and any("Reaproveitando" in a for a in avisos)


def test_nao_reaproveita_sessao_com_outra_configuracao(hub):
    inst = InstanceConfig(num_workers=4)
    glue = FakeGlueClient(session_states=["READY"], output=_saida(),
                          listed_sessions=[_anterior("sqs-outra", InstanceConfig(num_workers=10)),
                                           _anterior("sqs-de-outro", inst, dono="arn:aws:sts::1:assumed-role/X/y")])
    motor = _motor(glue, inst)
    hub.run(motor, "a", fingerprint=inst.fingerprint())
    assert len(glue.created) == 1
    assert [s["Id"] for s in hub.slot(motor).reusable] == ["sqs-outra"]     # fica oferecida na tela


def test_escolher_sessao_anterior_na_tela(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.adopt(motor, {"Id": "sqs-escolhida", "Status": "READY", "fingerprint": "x", "IdleTimeout": 60})
    hub.run(motor, "a")
    assert glue.created == [] and glue.statements[0][0] == "sqs-escolhida"


# --------------------------------------------------------------------------- segundo plano
def _espera(cond, limite=5):
    fim = time.monotonic() + limite
    while not cond() and time.monotonic() < fim:
        threading.Event().wait(0.01)
    assert cond()


def test_inicio_em_segundo_plano_e_a_execucao_usa_a_mesma_sessao(hub):
    glue = FakeGlueClient(session_states=["PROVISIONING", "PROVISIONING", "READY"], output=_saida())
    motor = _motor(glue)
    assert hub.start_background(motor) is True
    _espera(lambda: not hub.slot(motor).starting)
    slot = hub.slot(motor)
    assert slot.session_id == glue.created[0]["Id"] and slot.status == "READY" and slot.error is None
    hub.run(motor, "a")
    assert len(glue.created) == 1


def test_erro_no_inicio_em_segundo_plano_vai_para_a_tela(hub):
    motor = _motor(FakeGlueClient(), glue_role_arn="")
    hub.start_background(motor)
    _espera(lambda: not hub.slot(motor).starting)
    assert "IAM Role" in hub.slot(motor).error


def test_execucao_espera_a_sessao_que_ja_esta_subindo(hub):
    entrou, liberar = threading.Event(), threading.Event()
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    criar = glue.create_session

    def create_session_lento(**kw):
        entrou.set()
        liberar.wait(5)
        return criar(**kw)

    glue.create_session = create_session_lento
    motor = _motor(glue)
    hub.start_background(motor)
    assert entrou.wait(5)                       # o segundo plano está criando a sessão
    resultado = []
    t = threading.Thread(target=lambda: resultado.append(hub.run(_motor(glue), "a")))
    t.start()
    threading.Event().wait(0.05)
    assert not glue.statements                  # a execução está esperando, sem criar outra sessão
    liberar.set()
    t.join(5)
    assert resultado and resultado[0]["ok"]
    assert len(glue.created) == 1 and [c for _, c in glue.statements] == ["a"]


# --------------------------------------------------------------------------- tela
def test_status_da_tela_usa_cache(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    chamadas = len(glue.get_session_calls)
    assert hub.status(motor) == "READY" and hub.status(motor) == "READY"
    assert len(glue.get_session_calls) == chamadas
    _envelhece(hub, motor)
    glue.session_states = ["TIMEOUT"]
    assert hub.status(motor) == "TIMEOUT" and hub.slot(motor).session_id is None


def test_contas_diferentes_tem_sessoes_diferentes(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    hub.run(_motor(glue, profile="dev"), "a")
    hub.run(_motor(glue, profile="prod"), "b")
    assert len(glue.created) == 2


def test_encerrar(hub):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue)
    hub.run(motor, "a")
    sid = hub.slot(motor).session_id
    hub.stop(motor)
    assert glue.stopped == [sid] and hub.slot(motor).session_id is None


def test_horario_previsto_de_encerramento(hub, monkeypatch):
    glue = FakeGlueClient(session_states=["READY"], output=_saida())
    motor = _motor(glue, InstanceConfig(idle_timeout_min=30))
    monkeypatch.setattr(session_hub.time, "time", lambda: 1_000.0)
    hub.run(motor, "a")
    assert hub.slot(motor).expected_end() == 1_000.0 + 30 * 60

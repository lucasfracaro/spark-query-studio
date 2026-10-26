import json

import pytest

import settings_store
from engines import AwsAccount, InstanceConfig


@pytest.fixture
def config_tmp(tmp_path, monkeypatch):
    arquivo = tmp_path / "cfg" / "config.json"
    monkeypatch.setattr(settings_store, "CONFIG_DIR", arquivo.parent)
    monkeypatch.setattr(settings_store, "CONFIG_FILE", arquivo)
    return arquivo


def test_sem_arquivo_devolve_padrao(config_tmp):
    acc, inst = settings_store.load()
    assert acc == AwsAccount() and inst == InstanceConfig()


def test_ida_e_volta_sem_gravar_segredos(config_tmp):
    acc = AwsAccount(region="sa-east-1", auth_mode="keys", access_key="AKIAEXEMPLO",
                     secret_key="segredo-123", session_token="tok-456", glue_role_arn="arn:role")
    inst = InstanceConfig(worker_type="G.4X", num_workers=10, extra_args={"--x": "1"})
    settings_store.save(acc, inst)

    texto = config_tmp.read_text()
    for segredo in ("AKIAEXEMPLO", "segredo-123", "tok-456"):
        assert segredo not in texto

    acc2, inst2 = settings_store.load()
    assert acc2.region == "sa-east-1" and acc2.glue_role_arn == "arn:role"
    assert acc2.access_key == acc2.secret_key == acc2.session_token == ""
    assert inst2 == inst
    # o objeto original continua com as chaves (só o disco fica sem elas)
    assert acc.secret_key == "segredo-123"


def test_arquivo_corrompido(config_tmp):
    config_tmp.parent.mkdir(parents=True)
    config_tmp.write_text("{ isso não é json")
    assert settings_store.load() == (AwsAccount(), InstanceConfig())


def test_campos_desconhecidos_sao_ignorados(config_tmp):
    config_tmp.parent.mkdir(parents=True)
    config_tmp.write_text(json.dumps({"account": {"region": "eu-west-1", "campo_antigo": 1},
                                      "instance": {"num_workers": 3, "outro": True}}))
    acc, inst = settings_store.load()
    assert acc.region == "eu-west-1" and inst.num_workers == 3

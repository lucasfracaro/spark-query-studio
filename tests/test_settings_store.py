import json
import os
import stat
import sys

import pytest

import settings_store
from engines import AwsAccount, InstanceConfig

ROLE = "arn:aws:iam::123456789012:role/GlueDev"


@pytest.fixture
def config_tmp(tmp_path, monkeypatch):
    arquivo = tmp_path / "cfg" / "configuracao.toml"
    monkeypatch.setenv(settings_store.ENV_VAR, str(arquivo))
    monkeypatch.setattr(settings_store, "LEGACY_FILE", tmp_path / "legado" / "config.json")
    return arquivo


def _escreve(caminho, texto):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(texto, encoding="utf-8")


def test_sem_arquivo_devolve_padrao(config_tmp):
    lc = settings_store.load()
    assert lc.account == AwsAccount() and lc.instance == InstanceConfig()
    assert lc.source == "padrao" and lc.path == config_tmp and lc.ok


def test_variavel_de_ambiente_aponta_o_arquivo(config_tmp, monkeypatch):
    assert settings_store.config_path() == config_tmp
    monkeypatch.delenv(settings_store.ENV_VAR)
    assert settings_store.config_path() == settings_store.DEFAULT_FILE


def test_ida_e_volta_sem_gravar_segredos(config_tmp):
    acc = AwsAccount(region="sa-east-1", auth_mode="keys", access_key="AKIAEXEMPLO",
                     secret_key="segredo-123", session_token="tok-456", glue_role_arn=ROLE,
                     temp_dir="s3://b/tmp/")
    inst = InstanceConfig(worker_type="G.4X", num_workers=10, glue_version="4.0", idle_timeout_min=45,
                          extra_args={"--datalake-formats": "iceberg", "--conf": 'a=1 --conf b="x"'},
                          tags={"projeto": "sqs"}, lake_formation=True, start_on_open=True)
    settings_store.save(acc, inst)

    texto = config_tmp.read_text(encoding="utf-8")
    for segredo in ("AKIAEXEMPLO", "segredo-123", "tok-456"):
        assert segredo not in texto
    assert 'autenticacao = "chaves"' in texto and "lake_formation = true" in texto

    lc = settings_store.load()
    # sem as chaves no arquivo não é problema: elas são digitadas na tela a cada vez
    assert lc.source == "arquivo" and lc.error is None and lc.problems == []
    assert settings_store.check(lc.account) == ["Com autenticação por chaves, informe a access key e a secret key."]
    assert lc.account == AwsAccount(region="sa-east-1", auth_mode="keys", glue_role_arn=ROLE, temp_dir="s3://b/tmp/")
    assert lc.instance == inst
    assert acc.secret_key == "segredo-123"      # o objeto original continua com as chaves


def test_chaves_so_vao_para_o_arquivo_quando_pedido(config_tmp):
    acc = AwsAccount(region="sa-east-1", auth_mode="keys", access_key="AKIAEXEMPLO", secret_key="s3gr3do",
                     glue_role_arn=ROLE)
    settings_store.save(acc, InstanceConfig(), include_secrets=True)
    lc = settings_store.load()
    assert lc.has_secrets and lc.account.secret_key == "s3gr3do" and lc.ok
    if sys.platform != "win32":
        assert stat.S_IMODE(os.stat(config_tmp).st_mode) == 0o600     # só o dono lê


def test_database_padrao_no_arquivo(config_tmp):
    assert InstanceConfig().default_database == "workspace_db"            # vem preenchido por padrão
    config_tmp.parent.mkdir(parents=True, exist_ok=True)
    config_tmp.write_text('[instancia]\ndatabase_padrao = "analytics_db"\n', encoding="utf-8")
    assert settings_store.load().instance.default_database == "analytics_db"
    config_tmp.write_text('[instancia]\ndatabase_padrao = ""\n', encoding="utf-8")        # vazio = nenhum
    lc = settings_store.load()
    assert lc.instance.default_database == "" and lc.ok
    config_tmp.write_text('[instancia]\ndatabase_padrao = "meu db"\n', encoding="utf-8")
    lc = settings_store.load()
    assert lc.instance.default_database == "workspace_db" and "database_padrao" in lc.problems[0]
    settings_store.save(AwsAccount(), InstanceConfig(default_database="outro_db"))
    assert 'database_padrao = "outro_db"' in config_tmp.read_text(encoding="utf-8")
    assert settings_store.load().instance.default_database == "outro_db"
    assert settings_store.check(AwsAccount(), InstanceConfig(default_database="x-y"))


def test_exemplo_versionado_esta_em_dia():
    esperado = settings_store.to_toml(AwsAccount(), InstanceConfig())
    assert settings_store.EXAMPLE_FILE.read_text(encoding="utf-8") == esperado


def test_erro_de_digitacao_nao_derruba_e_mostra_a_linha(config_tmp):
    _escreve(config_tmp, '[aws]\nregiao = "sa-east-1"\nperfil = dev\n')
    lc = settings_store.load()
    assert lc.error and "line 3" in lc.error
    assert lc.account == AwsAccount() and lc.text.startswith("[aws]")
    assert not lc.ok


def test_valores_invalidos_viram_avisos_e_usam_o_padrao(config_tmp):
    _escreve(config_tmp, """
[aws]
regiao = "sa-east1"
autenticacao = "sso"
role = "arn:aws:iam::123:role/X"
role_glue = "arn:aws:iam::123:role/X"

[instancia]
tipo_worker = "g.2x"
workers = 1
versao_glue = "3.0"
minutos_sem_uso = "trinta"
lake_formation = "talvez"

[outra]
x = 1
""")
    lc = settings_store.load()
    assert lc.error is None
    texto = "\n".join(lc.problems)
    for trecho in ("`[outra]`", "`role` desconhecida", "`autenticacao` = 'sso'", "`workers` = 1",
                   "`versao_glue` = '3.0'", "`minutos_sem_uso` = 'trinta'", "`lake_formation`",
                   "Região `sa-east1`", "ARN da role"):
        assert trecho in texto, trecho
    assert lc.instance.worker_type == "G.2X"          # maiúsculas/minúsculas não importam
    assert lc.instance.num_workers == 2 and lc.instance.glue_version == "5.0"
    assert lc.account.auth_mode == "profile"


def test_aceita_os_nomes_em_ingles_da_autenticacao(config_tmp):
    _escreve(config_tmp, '[aws]\nregiao = "us-east-1"\nautenticacao = "default"\n')
    assert settings_store.load().account.auth_mode == "default"


def test_salvar_texto_recusa_texto_invalido_e_grava_o_valido(config_tmp):
    with pytest.raises(ValueError, match="erro de digitação"):
        settings_store.save_text("[aws\n")
    assert not config_tmp.exists()
    lc = settings_store.save_text('# meu comentário\n[aws]\nregiao = "eu-west-1"\n')
    assert lc.account.region == "eu-west-1"
    assert config_tmp.read_text(encoding="utf-8").startswith("# meu comentário")   # texto gravado como está


def test_perfil_inexistente(config_tmp):
    problemas = settings_store.check(AwsAccount(region="sa-east-1", profile="nao_existe"), profiles=["default", "dev"])
    assert problemas and "nao_existe" in problemas[0] and "default, dev" in problemas[0]


def test_le_o_formato_antigo_sem_segredos(config_tmp):
    settings_store.LEGACY_FILE.parent.mkdir(parents=True)
    settings_store.LEGACY_FILE.write_text(json.dumps({
        "account": {"region": "eu-west-1", "secret_key": "x", "campo_antigo": 1},
        "instance": {"num_workers": 3, "outro": True}}))
    lc = settings_store.load()
    assert lc.source == "legado" and lc.account.region == "eu-west-1" and lc.account.secret_key == ""
    assert lc.instance.num_workers == 3

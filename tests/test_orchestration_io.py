import json

import pytest

from code_builder import InputSource, Orchestration
from orchestration_io import OrchestrationFormatError, from_dict, from_json, to_json


def _orch():
    return Orchestration(
        inputs=[InputSource("vendas", "catalog", "db.vendas", filter="dt = '2026-10-26'", cache=True),
                InputSource("arq", "s3", "s3://b/p/", "csv", "header=true;sep=;"),
                InputSource("", "catalog", "")],                 # vazia: não vai para o arquivo
        spark_conf={"spark.sql.shuffle.partitions": "64"},
        default_database="analytics", limit=250)


def test_ida_e_volta():
    texto = to_json(_orch())
    volta = from_json(texto)
    assert volta.inputs == _orch().inputs[:2]
    assert volta.spark_conf == {"spark.sql.shuffle.partitions": "64"}
    assert volta.default_database == "analytics" and volta.limit == 250


def test_arquivo_legivel_em_portugues():
    data = json.loads(to_json(_orch()))
    assert set(data) == {"versao", "database_padrao", "limite_linhas", "spark_conf", "entradas"}
    assert "dt = '2026-10-26'" in to_json(_orch())          # sem escapar acentos/aspas à toa


def test_formato_antigo_da_aba_importar_exportar():
    antigo = {"inputs": [{"alias": "v", "kind": "catalog", "source": "db.v", "fmt": "parquet", "options": None,
                          "filter": "", "cache": "true"}],
              "conf": [{"chave": "spark.x", "valor": "1"}, {"chave": "", "valor": "ignorado"}],
              "default_db": "db", "limit": 50}
    o = from_dict(antigo)
    assert o.inputs == [InputSource("v", "catalog", "db.v", cache=True)]
    assert o.spark_conf == {"spark.x": "1"} and o.default_database == "db" and o.limit == 50


@pytest.mark.parametrize("texto, trecho", [
    ("{ não é json", "JSON inválido na linha 1"),
    ("[1, 2]", "objeto JSON"),
    ('{"entradas": {}}', "deve ser uma lista"),
    ('{"entradas": [1]}', "Entrada 1"),
    ('{"entradas": [{"alias": "x", "kind": "hdfs", "source": "a"}]}', "tipo 'hdfs'"),
    ('{"entradas": [{"alias": "x", "source": "a", "fmt": "xlsx"}]}', "formato 'xlsx'"),
    ('{"spark_conf": [1, 2], "entradas": []}', None),            # lista antiga com itens inválidos: ignorados
    ('{"spark_conf": "x"}', "spark_conf"),
    ('{"limite_linhas": "muitas"}', "número"),
])
def test_erros_de_formato(texto, trecho):
    if trecho is None:
        assert from_json(texto).spark_conf == {}
        return
    with pytest.raises(OrchestrationFormatError, match=trecho):
        from_json(texto)


def test_campos_extras_sao_ignorados():
    o = from_json('{"entradas": [{"alias": "v", "source": "db.v", "comentario": "x"}], "outro": 1}')
    assert o.inputs[0].alias == "v"

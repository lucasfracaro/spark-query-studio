import json

import pytest

from metadata import build_ddl, build_dictionary, dictionary_to_json, spark_to_athena_type


@pytest.mark.parametrize("spark, athena", [
    ("int", "int"),
    ("decimal(18,2)", "decimal(18,2)"),
    ("timestamp_ntz", "timestamp"),
    ("void", "string"),
    ("interval day to second", "string"),
    ("array<struct<a:int,b:timestamp_ntz>>", "array<struct<a:int,b:timestamp>>"),
    ("map<string,void>", "map<string,string>"),
    ("STRING", "string"),
    ("", "string"),
])
def test_tipos(spark, athena):
    assert spark_to_athena_type(spark) == athena


COLS = [{"name": "id", "type": "bigint", "nullable": False},
        {"name": "valor", "type": "decimal(10,2)", "nullable": True},
        {"name": "dt", "type": "string"}]


def test_dicionario_sem_estatisticas():
    rows = build_dictionary(COLS)
    assert [r["coluna"] for r in rows] == ["id", "valor", "dt"]
    assert rows[0]["aceita_nulo"] is False and rows[2]["aceita_nulo"] is True    # sem a informação: True
    assert "nulos" not in rows[0]


def test_dicionario_com_estatisticas():
    profile = {"rows": 200, "columns": [{"nulls": 0, "distinct": 200, "min": 1, "max": 200},
                                        {"nulls": 50, "distinct": 30, "min": "0.50", "max": "99.90"},
                                        {"nulls": 0, "distinct": 3, "min": "2026-10-01", "max": "2026-10-03"}]}
    rows = build_dictionary(COLS, profile)
    assert rows[1]["nulos"] == 50 and rows[1]["pct_nulos"] == 25.0
    assert rows[2]["minimo"] == "2026-10-01"


def test_dicionario_json():
    data = json.loads(dictionary_to_json(build_dictionary(COLS), "tb", 10))
    assert data["tabela"] == "tb" and data["linhas"] == 10 and len(data["colunas"]) == 3


def test_ddl_parquet_particionado():
    rows = build_dictionary(COLS)
    rows[1]["descricao"] = "Valor da venda (R$) — com 'aspas'"
    ddl = build_ddl(rows, "analytics", "vendas", "s3://bucket/vendas", ["dt"], "parquet", "Vendas diárias")
    assert ddl.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS `analytics`.`vendas` (")
    corpo, particao = ddl.split("PARTITIONED BY")
    assert "`id` bigint" in corpo and "`dt`" not in corpo
    assert "COMMENT 'Valor da venda (R$) — com \\'aspas\\''" in corpo
    assert "`dt` string" in particao
    assert "STORED AS PARQUET" in ddl and "LOCATION 's3://bucket/vendas/'" in ddl
    assert "COMMENT 'Vendas diárias'" in ddl and ddl.endswith(";")


def test_ddl_csv():
    ddl = build_ddl(build_dictionary(COLS), "", "tb", "s3://b/p/", fmt="csv")
    assert "`tb`" in ddl and "PARTITIONED" not in ddl
    assert "FIELDS TERMINATED BY ';'" in ddl and "skip.header.line.count" in ddl


@pytest.mark.parametrize("kw, erro", [
    ({"table": ""}, "nome da tabela"),
    ({"location": "/local/path"}, "s3://"),
    ({"partition_columns": ["nao_existe"]}, "não existem"),
    ({"partition_columns": ["id", "valor", "dt"]}, "ao menos uma coluna"),
])
def test_ddl_validacoes(kw, erro):
    args = dict(rows=build_dictionary(COLS), database="db", table="tb", location="s3://b/p/")
    args.update(kw)
    with pytest.raises(ValueError, match=erro):
        build_ddl(**args)

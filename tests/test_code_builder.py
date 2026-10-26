import base64
import json

import pytest

from code_builder import (RESULT_MARKER, InputSource, Orchestration, build_spark_code,
                          parse_options, parse_result, split_statements, validate_orchestration)
from fakes import FakeSpark, run_generated


# --------------------------------------------------------------------------- split_statements
@pytest.mark.parametrize("sql, esperado", [
    ("SELECT 1", ["SELECT 1"]),
    ("SELECT 1;", ["SELECT 1"]),
    ("SELECT 1; SELECT 2", ["SELECT 1", "SELECT 2"]),
    ("  ;; SELECT 1 ;\n\n ; ", ["SELECT 1"]),
    ("SELECT ';' AS a", ["SELECT ';' AS a"]),
    ('SELECT ";" AS a', ['SELECT ";" AS a']),
    ("SELECT `col;x` FROM t", ["SELECT `col;x` FROM t"]),
    ("SELECT 'it''s; ok'", ["SELECT 'it''s; ok'"]),
    ("SELECT 'it\\'s; ok'; SELECT 2", ["SELECT 'it\\'s; ok'", "SELECT 2"]),
    ("SELECT 1 -- comentário; com ponto e vírgula\n; SELECT 2", ["SELECT 1", "SELECT 2"]),
    ("SELECT /* ; */ 1; SELECT 2", ["SELECT  1", "SELECT 2"]),
    ("SELECT '--não é comentário' AS a", ["SELECT '--não é comentário' AS a"]),
    ("-- só comentário", []),
    ("", []),
])
def test_split_statements(sql, esperado):
    assert split_statements(sql) == esperado


def test_split_statements_comentario_de_bloco_sem_fim():
    assert split_statements("SELECT 1 /* nunca fecha ; SELECT 2") == ["SELECT 1"]


# --------------------------------------------------------------------------- parse_options
@pytest.mark.parametrize("texto, esperado", [
    ("", {}),
    (None, {}),
    ("header=true", {"header": "true"}),
    ("header=true;", {"header": "true"}),
    ("header=true; sep=,", {"header": "true", "sep": ","}),
    ("header=true;sep=;", {"header": "true", "sep": ";"}),
    ("sep=;;header=true", {"sep": ";", "header": "true"}),
    ("sep=;", {"sep": ";"}),
    ("dateFormat=yyyy-MM-dd;quote=\"", {"dateFormat": "yyyy-MM-dd", "quote": '"'}),
    ("path=a=b", {"path": "a=b"}),
    ("=sem_chave;lixo;x=1", {"x": "1"}),
])
def test_parse_options(texto, esperado):
    assert parse_options(texto) == esperado


# --------------------------------------------------------------------------- validate_orchestration
def _orch(*inputs, **kw):
    return Orchestration(inputs=list(inputs), **kw)


def test_validacao_ok():
    orch = _orch(InputSource("vendas", "catalog", "db.vendas"),
                 InputSource("clientes", "s3", "s3://bucket/clientes/", "csv"))
    assert validate_orchestration(orch) == []


@pytest.mark.parametrize("alias", ["1abc", "com espaço", "traço-no-meio", "ç"])
def test_alias_invalido(alias):
    erros = validate_orchestration(_orch(InputSource(alias, "catalog", "db.t")))
    assert any("Alias inválido" in e for e in erros)


def test_alias_duplicado_ignora_maiusculas():
    erros = validate_orchestration(_orch(InputSource("vendas", "catalog", "db.a"),
                                         InputSource("VENDAS", "catalog", "db.b")))
    assert any("duplicado" in e for e in erros)


def test_s3_exige_caminho_valido():
    erros = validate_orchestration(_orch(InputSource("v", "s3", "bucket/sem/protocolo")))
    assert any("s3://" in e for e in erros)


def test_catalog_sem_database():
    assert validate_orchestration(_orch(InputSource("v", "catalog", "tabela")))
    # com database padrão a tabela sozinha é aceita
    assert validate_orchestration(_orch(InputSource("v", "catalog", "tabela"), default_database="db")) == []


@pytest.mark.parametrize("limit", [0, 10001])
def test_limite_fora_da_faixa(limit):
    assert validate_orchestration(_orch(limit=limit))


def test_linhas_incompletas_sao_ignoradas():
    # linha vazia vinda do editor de tabela não deve gerar erro
    assert validate_orchestration(_orch(InputSource("", "catalog", ""))) == []


# --------------------------------------------------------------------------- build_spark_code
def test_query_vazia():
    with pytest.raises(ValueError):
        build_spark_code("  -- nada\n ; ", Orchestration())


def test_codigo_gerado_compila_com_caracteres_especiais():
    sql = "SELECT 'aspas \"duplas\"', 'acentuação ç', '\\n' AS x, `col` FROM t"
    code = build_spark_code(sql, Orchestration(inputs=[InputSource("t", "s3", "s3://b/p/", options="sep=;")]))
    compile(code, "<gerado>", "exec")


def test_execucao_completa():
    orch = Orchestration(
        inputs=[InputSource("vendas", "catalog", "db.vendas", filter="dt >= '2026-01-01'"),
                InputSource("clientes", "s3", "s3://b/clientes/", "csv", "header=true; sep=;", cache=True),
                InputSource("", "catalog", "")],        # linha vazia: ignorada
        spark_conf={"spark.sql.shuffle.partitions": "10", "": "ignorada"},
        default_database="analytics",
        limit=2,
    )
    spark = FakeSpark()
    res = run_generated(build_spark_code("SET x=1; SELECT * FROM vendas", orch), spark)

    assert res["ok"] is True
    assert [c["name"] for c in res["columns"]] == ["id", "nome", "valor"]
    assert res["rows"] == [[1, "a", None], [2, "b", 3.5]]           # limite aplicado
    assert spark.conf_values == {"spark.sql.shuffle.partitions": "10"}
    assert spark.sql_log == ["USE analytics", "SET x=1", "SELECT * FROM vendas"]
    assert set(spark.views) == {"vendas", "clientes"}
    assert ("where", "dt >= '2026-01-01'") in spark.calls
    assert ("load", "csv", "s3://b/clientes/", {"header": "true", "sep": ";"}) in spark.calls
    assert spark.cached == ["clientes"]
    assert any("view vendas" in linha for linha in res["log"])


def test_conf_invalida_vira_aviso_e_nao_interrompe():
    spark = FakeSpark(bad_conf={"spark.executor.memory"})
    orch = Orchestration(spark_conf={"spark.executor.memory": "8g"})
    res = run_generated(build_spark_code("SELECT 1", orch), spark)
    assert res["ok"] is True
    assert any(linha.startswith("AVISO") for linha in res["log"])


def test_erro_na_query_volta_como_resultado():
    spark = FakeSpark(fail_on="tabela_inexistente")
    res = run_generated(build_spark_code("SELECT * FROM tabela_inexistente", Orchestration()), spark)
    assert res["ok"] is False
    assert "TABLE_OR_VIEW_NOT_FOUND" in res["error"]
    assert "Traceback" in res["traceback"]


def test_tipos_nao_json_viram_texto():
    import datetime
    import decimal
    rows = [(decimal.Decimal("1.50"), datetime.date(2026, 10, 26), b"bin", [1, 2], True)]
    cols = [("d", "decimal(10,2)"), ("dt", "date"), ("b", "binary"), ("arr", "array<int>"), ("ok", "boolean")]
    res = run_generated(build_spark_code("SELECT 1", Orchestration()), FakeSpark(rows=rows, columns=cols))
    assert res["rows"] == [["1.50", "2026-10-26", "b'bin'", "[1, 2]", True]]


def test_query_viaja_intacta():
    # a query com aspas e quebras de linha chega ao Spark exatamente como foi escrita
    sql = "SELECT 'a''b' AS x,\n       \"y\" AS `z`\nFROM t"
    spark = FakeSpark()
    run_generated(build_spark_code(sql, Orchestration()), spark)
    assert spark.sql_log == [sql]


# --------------------------------------------------------------------------- parse_result
def _linha(obj):
    return RESULT_MARKER + base64.b64encode(json.dumps(obj).encode()).decode()


def test_parse_result_pega_a_ultima_marcacao():
    saida = "\n".join(["log qualquer", _linha({"n": 1}), "WARN algo", _linha({"n": 2}), ""])
    assert parse_result(saida) == {"n": 2}


def test_parse_result_marcacao_no_meio_da_linha():
    assert parse_result("prefixo " + _linha({"ok": True})) == {"ok": True}


@pytest.mark.parametrize("saida", ["", "sem marcação", RESULT_MARKER + "@@@não-é-base64", RESULT_MARKER + "e30"])
def test_parse_result_saida_invalida(saida):
    # "e30" é base64 de "{}" truncado: não deve estourar exceção
    assert parse_result(saida) in (None, {})

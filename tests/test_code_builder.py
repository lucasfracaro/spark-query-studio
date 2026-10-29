import base64
import json
import random
import zlib

import pytest

import code_builder
from code_builder import (GLUE_CODE_LIMIT, RESULT_MARKER, InputSource, Orchestration, build_spark_code,
                          parse_options, parse_result, py_literal, split_statements, validate_orchestration)
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
    ("SELECT 1 -- comentário; com ponto e vírgula\n; SELECT 2",
     ["SELECT 1 -- comentário; com ponto e vírgula", "SELECT 2"]),
    ("SELECT /* ; */ 1; SELECT 2", ["SELECT /* ; */ 1", "SELECT 2"]),
    ("SELECT '--não é comentário' AS a", ["SELECT '--não é comentário' AS a"]),
    ("-- só comentário", []),
    ("SELECT 1; -- comentário depois do último comando\n/* outro */", ["SELECT 1"]),
    ("", []),
])
def test_split_statements(sql, esperado):
    assert split_statements(sql) == esperado


def test_split_statements_comentario_de_bloco_sem_fim():
    assert split_statements("SELECT 1 /* nunca fecha ; SELECT 2") == ["SELECT 1 /* nunca fecha ; SELECT 2"]


@pytest.mark.parametrize("sql", [
    "SELECT /*+ BROADCAST(b) */ * FROM a JOIN b ON a.id = b.id",
    "SELECT valor/*comentário*/AS total FROM t",
    "-- cabeçalho do arquivo\nSELECT 1",
])
def test_comentarios_e_hints_chegam_intactos_ao_spark(sql):
    assert split_statements(sql) == [sql]
    spark = FakeSpark()
    run_generated(build_spark_code(sql, Orchestration()), spark)
    assert spark.last_session.sql_log == [sql]


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
    s = spark.last_session                                          # tudo roda numa sessão nova
    assert s.conf_values == {"spark.sql.catalogImplementation": "hive", "spark.sql.shuffle.partitions": "10"}
    assert s.sql_log == ["USE analytics", "SET x=1", "SELECT * FROM vendas"]
    assert set(s.views) == {"vendas", "clientes"}
    assert ("where", "dt >= '2026-01-01'") in s.calls
    assert ("load", "csv", "s3://b/clientes/", {"header": "true", "sep": ";"}) in s.calls
    assert s.cached == ["clientes"]
    assert any("view vendas" in linha for linha in res["log"])
    # a sessão original não é alterada
    assert spark.conf_values == {} and spark.views == {} and spark.sql_log == ["SET"]


def test_cada_execucao_usa_sessao_nova_e_limpa_o_cache():
    spark = FakeSpark(settings={"spark.sql.catalogImplementation": "hive", "spark.glue.algo": "x"})
    orch = Orchestration(inputs=[InputSource("v", "catalog", "db.v")],
                         spark_conf={"spark.sql.shuffle.partitions": "7"})
    run_generated(build_spark_code("SELECT * FROM v", orch), spark)
    run_generated(build_spark_code("SELECT 1", Orchestration()), spark)
    primeira, segunda = spark.sessions
    assert primeira is not segunda
    assert "v" in primeira.views and segunda.views == {}            # a view da 1ª execução não existe na 2ª
    assert segunda.conf_values == {"spark.sql.catalogImplementation": "hive", "spark.glue.algo": "x"}
    assert primeira.cache_cleared == 1 and segunda.cache_cleared == 1


def test_comandos_intermediarios_nao_sao_trazidos_para_o_driver():
    spark = FakeSpark()
    sql = "CREATE TEMP VIEW a AS SELECT 1; SELECT * FROM grande; SELECT 2"
    run_generated(build_spark_code(sql, Orchestration()), spark)
    assert spark.last_session.calls.count(("collect",)) == 1        # só o resultado final


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
    assert spark.last_session.sql_log == [sql]


def test_quebras_de_linha_do_windows_sao_normalizadas():
    spark = FakeSpark()
    run_generated(build_spark_code("SELECT 1\r\nFROM t", Orchestration()), spark)
    assert spark.last_session.sql_log == ["SELECT 1\nFROM t"]


# --------------------------------------------------------------------------- código legível e limite do Glue
def test_codigo_gerado_e_legivel():
    sql = "SELECT cliente,\n       SUM(valor) AS total\nFROM vendas\nGROUP BY cliente"
    orch = Orchestration(inputs=[InputSource("vendas", "catalog", "db.vendas", filter="dt = '2026-10-29'")])
    code = build_spark_code(sql, orch)
    assert sql in code                                 # a query aparece como foi escrita
    assert "'filter': \"dt = '2026-10-29'\"" in code
    assert "b64decode" not in code.split("def _emit")[0]


@pytest.mark.parametrize("texto", [
    "simples", "", "it's", "termina com aspas'", "barra no fim\\", "três " + "'" * 3 + "aspas",
    "linha\r\nwindows", "barra \\' no meio", '"duplas"', "acentuação çã", "tab\there", "nulo\x00",
])
def test_py_literal_ida_e_volta(texto):
    assert eval(py_literal(texto)) == texto


def test_py_literal_textos_aleatorios():
    rnd = random.Random(42)
    alfabeto = ["'", '"', "\\", "\n", "a", " ", "\r", "`", "{", "}"]
    for _ in range(3000):
        texto = "".join(rnd.choice(alfabeto) for _ in range(rnd.randint(0, 12)))
        assert eval(py_literal(texto)) == texto, repr(texto)


def test_query_grande_vai_comprimida_e_continua_funcionando():
    linha = "  COALESCE(t.coluna_exemplo, 'x') AS coluna_exemplo_{},\n"
    sql = "SELECT\n" + "".join(linha.format(i) for i in range(1500)) + "  1 AS fim\nFROM t"
    assert len(sql) > GLUE_CODE_LIMIT
    code = build_spark_code(sql, Orchestration())
    assert len(code) <= GLUE_CODE_LIMIT and "_zlib.decompress" in code
    spark = FakeSpark()
    assert run_generated(code, spark)["ok"] is True
    assert spark.last_session.sql_log == [sql]


def test_query_grande_demais_mesmo_comprimida(monkeypatch):
    monkeypatch.setattr(code_builder, "GLUE_CODE_LIMIT", 2000)
    with pytest.raises(ValueError, match="grande demais"):
        build_spark_code("SELECT " + ", ".join(f"c{i}" for i in range(3000)) + " FROM t", Orchestration())


# --------------------------------------------------------------------------- parse_result
def _linha(obj):
    return RESULT_MARKER + base64.b64encode(json.dumps(obj).encode()).decode()


def test_parse_result_pega_a_ultima_marcacao():
    saida = "\n".join(["log qualquer", _linha({"n": 1}), "WARN algo", _linha({"n": 2}), ""])
    assert parse_result(saida) == {"n": 2}


def test_parse_result_marcacao_no_meio_da_linha():
    assert parse_result("prefixo " + _linha({"ok": True})) == {"ok": True}


def test_parse_result_comprimido():
    linha = RESULT_MARKER + base64.b64encode(zlib.compress(json.dumps({"ok": True}).encode())).decode()
    assert parse_result("log\n" + linha) == {"ok": True}


@pytest.mark.parametrize("saida", ["", "sem marcação", RESULT_MARKER + "@@@não-é-base64", RESULT_MARKER + "e30"])
def test_parse_result_saida_invalida(saida):
    # "e30" é base64 de "{}" truncado: não deve estourar exceção
    assert parse_result(saida) in (None, {})


def test_estatisticas_desligadas_por_padrao_e_nullable_no_resultado():
    res = run_generated(build_spark_code("SELECT 1", Orchestration()), FakeSpark())
    assert "profile" not in res
    assert all(c["nullable"] is True for c in res["columns"])


def test_codigo_com_estatisticas_compila():
    compile(build_spark_code("SELECT 1", Orchestration(), profile=True), "<gerado>", "exec")

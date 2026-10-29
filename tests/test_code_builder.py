import base64
import json
import random
import zlib

import pytest

import code_builder
from code_builder import (
    GLUE_CODE_LIMIT,
    RESULT_MARKER,
    InputSource,
    Orchestration,
    build_clear_cache_code,
    build_spark_code,
    input_cache_key,
    parse_options,
    parse_result,
    py_literal,
    split_statements,
    sql_cache_targets,
    validate_orchestration,
)
from fakes import FakeGlueContext, FakeSpark, run_generated


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
    assert spark.cached == [("load", "csv", "s3://b/clientes/", (("header", "true"), ("sep", ";")))]
    assert any("view vendas" in linha for linha in res["log"])
    # a sessão original não é alterada
    assert spark.conf_values == {} and spark.views == {} and spark.sql_log == ["SET"]


def test_cada_execucao_usa_sessao_nova():
    spark = FakeSpark(settings={"spark.sql.catalogImplementation": "hive", "spark.glue.algo": "x"})
    orch = Orchestration(inputs=[InputSource("v", "catalog", "db.v")],
                         spark_conf={"spark.sql.shuffle.partitions": "7"})
    run_generated(build_spark_code("SELECT * FROM v", orch), spark)
    run_generated(build_spark_code("SELECT 1", Orchestration()), spark)
    primeira, segunda = spark.sessions
    assert primeira is not segunda
    assert "v" in primeira.views and segunda.views == {}            # a view da 1ª execução não existe na 2ª
    assert segunda.conf_values == {"spark.sql.catalogImplementation": "hive", "spark.glue.algo": "x"}
    assert spark.cache_cleared == 0                                 # o cache não é jogado fora a cada execução


# --------------------------------------------------------------------------- cache entre execuções
def _cacheada(alias="clientes", filtro="", fonte="s3://b/clientes/"):
    return InputSource(alias, "s3", fonte, "csv", "header=true", filter=filtro, cache=True)


def _rodar(spark, ns, sql="SELECT 1", *inputs, **kw):
    return run_generated(build_spark_code(sql, Orchestration(inputs=list(inputs)), **kw), spark, ns)


def test_entrada_em_cache_nao_e_relida_na_execucao_seguinte():
    spark, ns = FakeSpark(), {}
    r1 = _rodar(spark, ns, "SELECT * FROM clientes", _cacheada())
    assert r1["ok"] and any("lido agora" in linha for linha in r1["log"])
    r2 = _rodar(spark, ns, "SELECT count(*) FROM clientes", _cacheada())
    assert r2["ok"] and any("cache reaproveitado" in linha for linha in r2["log"])
    assert len(spark.cached) == 1 and spark.uncached == []        # leu e guardou uma vez só


def test_renomear_o_alias_nao_obriga_a_reler():
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", _cacheada("a"))
    r2 = _rodar(spark, ns, "SELECT 1", _cacheada("b"))
    assert any("cache reaproveitado" in linha for linha in r2["log"]) and len(spark.cached) == 1


def test_entrada_que_sai_da_orquestracao_e_liberada():
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", _cacheada())
    r2 = _rodar(spark, ns, "SELECT 1", InputSource("outra", "catalog", "db.t"))
    assert spark.uncached == spark.cached and spark.shared_cache == set()
    assert "cache liberado: clientes" in r2["log"]


def test_filtro_diferente_libera_o_antigo_e_le_de_novo():
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", _cacheada(filtro="dt = '1'"))
    _rodar(spark, ns, "SELECT 1", _cacheada(filtro="dt = '2'"))
    antigo, novo = spark.cached
    assert spark.uncached == [antigo] and spark.shared_cache == {novo}


def test_cache_que_o_spark_perdeu_e_refeito():
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", _cacheada())
    spark.shared_cache.clear()                     # ex.: alguém rodou CLEAR CACHE na sessão
    r2 = _rodar(spark, ns, "SELECT 1", _cacheada())
    assert len(spark.cached) == 2 and any("lido agora" in linha for linha in r2["log"])


def test_cache_table_da_query_fica_enquanto_o_comando_continuar_na_query():
    spark, ns = FakeSpark(), {}
    sql = "-- base\nCACHE TABLE tmp AS SELECT 1; SELECT * FROM tmp"
    _rodar(spark, ns, sql)
    _rodar(spark, ns, sql + " WHERE 1 = 1")       # o CACHE TABLE é o mesmo: continua
    assert spark.uncached == []
    r3 = _rodar(spark, ns, "SELECT 2")
    assert spark.uncached == [("table", "tmp")] and "cache liberado: tmp" in r3["log"]


def test_limpar_cache():
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", _cacheada())
    res = run_generated(build_clear_cache_code(), spark, ns)
    assert res["ok"] and "1 entrada" in res["log"][0]
    assert spark.cache_cleared == 1 and ns["_sqs_cache"] == {}


def test_chave_do_cache():
    base = input_cache_key(_cacheada("a"))
    assert base == input_cache_key(_cacheada("b"))                                  # alias não importa
    assert base != input_cache_key(_cacheada(filtro="x = 1"))
    assert base != input_cache_key(_cacheada(fonte="s3://b/outro/"))
    tabela = InputSource("v", "catalog", "vendas")
    assert input_cache_key(tabela, "db1") != input_cache_key(tabela, "db2")         # sem database: o padrão conta
    assert input_cache_key(tabela, "db1") != input_cache_key(tabela, "db1", "glue_context")
    completa = InputSource("v", "catalog", "db.vendas")
    assert input_cache_key(completa, "db1") == input_cache_key(completa, "db2")


@pytest.mark.parametrize("sql, nomes", [
    ("CACHE TABLE t AS SELECT 1", ["t"]),
    ("-- comentário\n/* outro */ cache lazy table db.t", ["db.t"]),
    ("CACHE TABLE `minha view` OPTIONS ('storageLevel' 'DISK_ONLY') SELECT 1", ["`minha view`"]),
    ("SELECT 'CACHE TABLE x'", []),
    ("UNCACHE TABLE t", []),
])
def test_cache_table_na_query(sql, nomes):
    assert [a["name"] for a in sql_cache_targets([sql])] == nomes


# --------------------------------------------------------------------------- Glue 4.0 + Lake Formation
@pytest.fixture
def glue_context(monkeypatch):
    import sys
    import types
    pacote, modulo = types.ModuleType("awsglue"), types.ModuleType("awsglue.context")
    modulo.GlueContext = FakeGlueContext
    pacote.context = modulo
    monkeypatch.setitem(sys.modules, "awsglue", pacote)
    monkeypatch.setitem(sys.modules, "awsglue.context", modulo)
    FakeGlueContext.instancias.clear()
    return FakeGlueContext.instancias


def test_glue_context_usa_a_leitura_nativa_com_push_down_predicate(glue_context):
    spark, ns = FakeSpark(), {}
    orch = Orchestration(inputs=[InputSource("vendas", "catalog", "db.vendas", filter="dt >= '2026-10-01'"),
                                 InputSource("itens", "catalog", "itens"),
                                 InputSource("arquivo", "s3", "s3://b/p/")],
                         default_database="analytics")
    res = run_generated(build_spark_code("SELECT * FROM vendas", orch, reader="glue_context"), spark, ns)
    assert res["ok"], res
    [gc] = glue_context
    assert gc.native_reads == [("db", "vendas", "dt >= '2026-10-01'", {"useSparkDataSource": True}),
                               ("analytics", "itens", "", {"useSparkDataSource": True})]
    assert gc.reads == []                                          # o DynamicFrame não foi preciso
    s = spark.last_session
    vendas = s.views["vendas"]
    assert vendas.spark is s                                       # usado na sessão da execução...
    assert vendas.plan[:2] == ("global", "Relation")               # ...pela view global, com o mesmo plano
    assert vendas.plan[-1] == ("where", "dt >= '2026-10-01'")
    assert s.views["arquivo"].plan[0] == "load"                    # S3 direto continua igual
    assert any("leitura nativa do Spark" in linha for linha in res["log"])


def test_glue_context_sem_leitura_nativa_usa_o_dynamic_frame(glue_context):
    spark, ns = FakeSpark(), {}
    _rodar(spark, ns, "SELECT 1", InputSource("x", "catalog", "db.x"), reader="glue_context")   # cria o GlueContext
    glue_context[0].nativa_falha = True
    res = _rodar(spark, ns, "SELECT 1", InputSource("v", "catalog", "db.v", filter="dt = '1'"), reader="glue_context")
    assert res["ok"], res
    assert glue_context[0].reads == [("db", "v", "dt = '1'")]
    assert any("DynamicFrame" in linha for linha in res["log"])
    assert any("a leitura nativa não funcionou para v" in linha for linha in res["log"])


def test_glue_context_filtro_fora_das_particoes_para_em_vez_de_ler_tudo(glue_context):
    spark, ns = FakeSpark(), {}
    r1 = _rodar(spark, ns, "SELECT 1", InputSource("v", "catalog", "db.v"), reader="glue_context")
    assert any("SEM filtro" in linha for linha in r1["log"]) and any("dica: marque Cache" in linha for linha in r1["log"])
    glue_context[0].falhar_com_filtro = True
    res = _rodar(spark, ns, "SELECT 1", InputSource("v", "catalog", "db.v", filter="valor > 10"),
                 reader="glue_context")
    assert res["ok"] is False
    assert "só colunas de partição" in res["error"] and "valor > 10" in res["error"]
    assert glue_context[0].reads == []                             # não leu a tabela inteira por conta própria


def test_glue_context_criado_uma_vez_e_cache_sem_reler(glue_context):
    spark, ns = FakeSpark(), {}
    entrada = InputSource("v", "catalog", "db.v", filter="dt = '1'", cache=True)
    _rodar(spark, ns, "SELECT 1", entrada, reader="glue_context")
    r2 = _rodar(spark, ns, "SELECT 2", entrada, reader="glue_context")
    assert len(glue_context) == 1 and len(glue_context[0].native_reads) == 1
    assert any("cache reaproveitado" in linha for linha in r2["log"])
    chave = next(iter(ns["_sqs_cache"]))
    assert "sqs_" + chave[2:] in spark.global_views
    _rodar(spark, ns, "SELECT 3", InputSource("v", "catalog", "db.v", filter="dt = '1'"), reader="glue_context")
    assert spark.uncached and len(glue_context[0].native_reads) == 2      # sem cache: liberou e leu de novo
    assert "sqs_" + chave[2:] not in spark.global_views or chave not in ns["_sqs_cache"]


def test_leitor_desconhecido():
    with pytest.raises(ValueError):
        build_spark_code("SELECT 1", Orchestration(), reader="outro")


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


def test_decimal_sem_notacao_cientifica():
    import decimal
    rows = [(decimal.Decimal("0E-10"), decimal.Decimal("1E+2"))]
    res = run_generated(build_spark_code("SELECT 1", Orchestration()),
                        FakeSpark(rows=rows, columns=[("a", "decimal(20,10)"), ("b", "decimal(10,0)")]))
    assert res["rows"] == [["0.0000000000", "100"]]


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

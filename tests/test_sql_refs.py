import pytest

import sql_refs
from code_builder import InputSource
from sql_refs import (check_query_against_orchestration, find_tables, find_write_statements,
                      missing_from_spark_error)

# Cada teste de leitura roda nos dois caminhos: com o sqlglot e só com o leitor de tokens (reserva).
MODOS = ["sqlglot", "tokens"]


def _find(sql, modo):
    if modo == "sqlglot" and sql_refs.sqlglot is None:
        pytest.skip("sqlglot não instalado")
    original = sql_refs.sqlglot
    sql_refs._find_tables_cached.cache_clear()
    if modo == "tokens":
        sql_refs.sqlglot = None
    try:
        return find_tables(sql)
    finally:
        sql_refs.sqlglot = original
        sql_refs._find_tables_cached.cache_clear()


def _refs(sql, modo):
    return [r.name for r in _find(sql, modo).refs]


def _casos(*casos):
    return [(*caso, modo) for caso in casos for modo in MODOS]


# --------------------------------------------------------------------------- find_tables
@pytest.mark.parametrize("sql, esperado, modo", _casos(
    ("SELECT * FROM vendas", ["vendas"]),
    ("select * from DB.Vendas", ["db.vendas"]),
    ("SELECT * FROM `db`.`minha tabela`", ["db.minha tabela"]),
    ("SELECT * FROM a JOIN b ON a.id=b.id LEFT OUTER JOIN c USING (id)", ["a", "b", "c"]),
    ("SELECT * FROM a, b x, c AS y WHERE 1=1", ["a", "b", "c"]),
    ("select x\nfrom\n  db.tab1 a,\n  db.tab2 b\nwhere a.k = b.k", ["db.tab1", "db.tab2"]),
    ("SELECT * FROM t WHERE id IN (SELECT id FROM t2) AND EXISTS (SELECT 1 FROM t3)", ["t", "t2", "t3"]),
    ("SELECT * FROM (SELECT * FROM interna) sub JOIN outra o ON 1=1", ["interna", "outra"]),
    ("SELECT * FROM db.t1 UNION ALL SELECT * FROM db.t2", ["db.t1", "db.t2"]),
    ("INSERT OVERWRITE TABLE destino SELECT * FROM origem", ["origem"]),
    ("INSERT INTO db.destino (a, b) SELECT a, b FROM origem", ["origem"]),
    ("SELECT * FROM t TABLESAMPLE (10 PERCENT)", ["t"]),
    ("SELECT * FROM t1 LATERAL VIEW explode(arr) e AS item", ["t1"]),
    ("SELECT * FROM a CROSS JOIN LATERAL (SELECT 1) l", ["a"]),
    ("SELECT * FROM a t(x, y) JOIN b ON 1=1", ["a", "b"]),
    ("MERGE INTO alvo t USING db.origem s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *", ["db.origem"]),
    ("TABLE db.tabela", ["db.tabela"]),
    ("SELECT /*+ BROADCAST(c) */ * FROM a JOIN b c ON a.id = c.id", ["a", "b"]),
    ("CACHE TABLE c AS SELECT * FROM x", ["x"]),
))
def test_encontra_bases(sql, esperado, modo):
    assert _refs(sql, modo) == esperado


def test_join_entre_parenteses():
    # só o sqlglot entende joins aninhados entre parênteses
    assert _refs("SELECT * FROM t1 JOIN (t2 JOIN t3 ON 1=1) ON 1=1", "sqlglot") == ["t1", "t2", "t3"]


@pytest.mark.parametrize("sql, modo", _casos(
    ("SELECT EXTRACT(YEAR FROM dt), TRIM(BOTH ' ' FROM nome), SUBSTRING(x FROM 2), POSITION('a' IN x) FROM t",),
    ("SELECT 'FROM falsa', \"JOIN tb\" FROM t -- FROM comentario\n/* JOIN outra */",),
    ("SELECT 'it''s FROM x', 'a\\'b FROM y' FROM t",),
))
def test_ignora_falsos_from(sql, modo):
    assert _refs(sql, modo) == ["t"]


@pytest.mark.parametrize("sql, modo", _casos(
    ("SELECT * FROM range(10)",),
    ("SELECT * FROM parquet.`s3://bucket/pasta/`",),
    ("SELECT * FROM csv.`/tmp/arquivo.csv`",),
    ("SHOW TABLES FROM meu_db",),
    ("SHOW PARTITIONS db.t",),
    ("SET spark.sql.shuffle.partitions=10",),
    ("USE meu_db",),
    ("DESCRIBE db.t",),
    ("SELECT 1",),
    ("",),
))
def test_sem_bases(sql, modo):
    assert _refs(sql, modo) == []


@pytest.mark.parametrize("modo", MODOS)
def test_ctes_e_views_criadas_contam_como_definidas(modo):
    sql = """
        CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM fonte;
        WITH base AS (SELECT * FROM tmp), outra (c1) AS (SELECT 1)
        SELECT * FROM base JOIN outra ON 1=1
    """
    found = _find(sql, modo)
    assert found.defined == {"tmp", "base", "outra"}
    assert [r.name for r in found.refs] == ["fonte", "tmp", "base", "outra"]


@pytest.mark.parametrize("modo", MODOS)
def test_numero_da_linha(modo):
    sql = "SELECT *\nFROM a\n\nJOIN b ON 1=1;\nSELECT 1 FROM c"
    assert [(r.name, r.line) for r in _find(sql, modo).refs] == [("a", 2), ("b", 4), ("c", 5)]


def test_comando_que_o_sqlglot_nao_entende_usa_o_leitor_de_tokens():
    # TABLE x não é entendido pelo sqlglot; o resto da query continua sendo lido por ele
    sql = "SELECT * FROM t1 JOIN (t2 JOIN t3 ON 1=1) ON 1=1;\nTABLE db.outra"
    assert [(r.name, r.line) for r in _find(sql, "sqlglot").refs] == [("t1", 1), ("t2", 1), ("t3", 1), ("db.outra", 2)]


def test_resultado_em_cache_nao_e_compartilhado():
    a = find_tables("SELECT * FROM x")
    a.refs.clear()
    a.defined.add("lixo")
    b = find_tables("SELECT * FROM x")
    assert [r.name for r in b.refs] == ["x"] and "lixo" not in b.defined


# --------------------------------------------------------------------------- conferência
INPUTS = [
    InputSource("vendas", "catalog", "db_vendas.tb_vendas", filter="dt = '2026-10-26'"),
    InputSource("clientes", "s3", "s3://bucket/clientes/", "csv"),
    InputSource("", "catalog", ""),                      # linha vazia: ignorada
]


def test_tudo_na_orquestracao():
    sql = "WITH v AS (SELECT * FROM vendas) SELECT * FROM v JOIN CLIENTES c ON v.id = c.id"
    check = check_query_against_orchestration(sql, INPUTS)
    assert check.ok and check.missing == [] and check.warnings == []


def test_base_fora_da_orquestracao():
    sql = "SELECT *\nFROM vendas v\nJOIN db_cad.produtos p ON v.pid = p.id\nJOIN db_cad.produtos p2 ON 1=1"
    check = check_query_against_orchestration(sql, INPUTS)
    assert not check.ok
    assert [(m.name, m.lines) for m in check.missing] == [("db_cad.produtos", [3, 4])]
    msg = check.missing[0].message()
    assert "`db_cad.produtos`" in msg and "linha 3, 4" in msg and "não está na orquestração" in msg


def test_nome_completo_em_vez_do_alias_gera_aviso_e_nao_erro():
    check = check_query_against_orchestration("SELECT * FROM db_vendas.tb_vendas", INPUTS)
    assert check.ok
    assert len(check.warnings) == 1
    assert "`vendas`" in check.warnings[0] and "filtro de partição" in check.warnings[0]


def test_varias_bases_faltando_sem_repetir():
    check = check_query_against_orchestration("SELECT * FROM x JOIN y ON 1=1 JOIN x ON 1=1", [])
    assert [m.name for m in check.missing] == ["x", "y"]
    assert check.missing[0].lines == [1]


def test_merge_cobra_a_origem():
    check = check_query_against_orchestration("MERGE INTO alvo USING db.origem s ON 1=1 WHEN MATCHED THEN DELETE", [])
    assert [m.name for m in check.missing] == ["db.origem"]


# --------------------------------------------------------------------------- modo somente leitura
@pytest.mark.parametrize("sql, palavra", [
    ("INSERT INTO db.t SELECT * FROM x", "INSERT"),
    ("insert overwrite table db.t select 1", "INSERT"),
    ("CREATE TABLE db.nova AS SELECT 1", "CREATE"),
    ("CREATE OR REPLACE VIEW db.v AS SELECT 1", "CREATE"),
    ("CREATE DATABASE teste", "CREATE"),
    ("DROP TABLE db.t", "DROP"),
    ("DROP VIEW v", "DROP"),
    ("ALTER TABLE db.t ADD COLUMNS (x INT)", "ALTER"),
    ("TRUNCATE TABLE db.t", "TRUNCATE"),
    ("DELETE FROM db.t WHERE dt = '1'", "DELETE"),
    ("UPDATE db.t SET x = 1", "UPDATE"),
    ("MERGE INTO db.t USING s ON 1=1 WHEN MATCHED THEN DELETE", "MERGE"),
    ("MSCK REPAIR TABLE db.t", "MSCK"),
    ("ANALYZE TABLE db.t COMPUTE STATISTICS", "ANALYZE"),
    ("WITH x AS (SELECT 1) INSERT INTO db.t SELECT * FROM x", "INSERT"),
    ("FROM db.origem INSERT OVERWRITE TABLE db.t SELECT *", "INSERT"),
    ("-- comentário antes\nINSERT INTO db.t VALUES (1)", "INSERT"),
])
def test_comandos_que_gravam(sql, palavra):
    achados = find_write_statements(sql)
    assert [w.keyword for w in achados] == [palavra]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM t",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "CREATE TEMP VIEW v AS SELECT 1",
    "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1",
    "CREATE GLOBAL TEMPORARY VIEW v AS SELECT 1",
    "CREATE TEMPORARY FUNCTION f AS 'com.x.F'",
    "DROP TEMPORARY FUNCTION f",
    "CACHE TABLE c AS SELECT * FROM t",
    "UNCACHE TABLE c",
    "SET spark.sql.shuffle.partitions=10",
    "USE db",
    "SHOW TABLES",
    "DESCRIBE db.t",
    "EXPLAIN INSERT INTO db.t SELECT 1",
    "SELECT 'INSERT INTO x' AS texto FROM t -- DROP TABLE y",
    "SELECT `insert` FROM t",
    "TABLE db.t",
])
def test_comandos_permitidos_no_modo_somente_leitura(sql):
    assert find_write_statements(sql) == []


def test_mensagem_do_modo_somente_leitura():
    sql = "CREATE TEMP VIEW a AS SELECT 1;\n\n-- grava\nINSERT INTO db.destino\nSELECT * FROM a"
    [w] = find_write_statements(sql)
    assert (w.number, w.line, w.keyword) == (2, 3, "INSERT")
    assert w.text == "INSERT INTO db.destino SELECT * FROM a"
    msg = w.message()
    assert "comando 2 (linha 3)" in msg and "somente leitura" in msg


# --------------------------------------------------------------------------- erro do Spark
@pytest.mark.parametrize("mensagem, esperado", [
    ("[TABLE_OR_VIEW_NOT_FOUND] The table or view `db`.`produtos` cannot be found. Verify the spelling",
     ("tabela", "db.produtos")),
    ("AnalysisException: Table or view not found: db.produtos; line 1 pos 14", ("tabela", "db.produtos")),
    ("Table or view not found: produtos; line 1 pos 14", ("tabela", "produtos")),
    ("[SCHEMA_NOT_FOUND] The schema `db_x` cannot be found.", ("banco", "db_x")),
    ("AnalysisException: Database 'db_x' not found", ("banco", "db_x")),
    ("[UNRESOLVED_COLUMN] A column with name `x` cannot be resolved", None),
    ("", None),
])
def test_traduz_erro_do_spark(mensagem, esperado):
    assert missing_from_spark_error(mensagem) == esperado

import pytest

from code_builder import InputSource
from sql_refs import check_query_against_orchestration, find_tables, missing_from_spark_error


def _refs(sql):
    return [r.name for r in find_tables(sql).refs]


# --------------------------------------------------------------------------- find_tables
@pytest.mark.parametrize("sql, esperado", [
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
    ("SELECT * FROM t TABLESAMPLE (10 PERCENT)", ["t"]),
    ("SELECT * FROM t1 LATERAL VIEW explode(arr) e AS item", ["t1"]),
    ("SELECT * FROM a CROSS JOIN LATERAL (SELECT 1) l", ["a"]),
    ("SELECT * FROM a t(x, y) JOIN b ON 1=1", ["a", "b"]),
])
def test_encontra_bases(sql, esperado):
    assert _refs(sql) == esperado


@pytest.mark.parametrize("sql", [
    "SELECT EXTRACT(YEAR FROM dt), TRIM(BOTH ' ' FROM nome), SUBSTRING(x FROM 2), POSITION('a' IN x) FROM t",
    "SELECT 'FROM falsa', \"JOIN tb\" FROM t -- FROM comentario\n/* JOIN outra */",
    "SELECT 'it''s FROM x', 'a\\'b FROM y' FROM t",
])
def test_ignora_falsos_from(sql):
    assert _refs(sql) == ["t"]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM range(10)",
    "SELECT * FROM parquet.`s3://bucket/pasta/`",
    "SELECT * FROM csv.`/tmp/arquivo.csv`",
    "SHOW TABLES FROM meu_db",
    "SHOW PARTITIONS db.t",
    "SET spark.sql.shuffle.partitions=10",
    "USE meu_db",
    "DESCRIBE db.t",
    "SELECT 1",
    "",
])
def test_sem_bases(sql):
    assert _refs(sql) == []


def test_ctes_e_views_criadas_contam_como_definidas():
    sql = """
        CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM fonte;
        WITH base AS (SELECT * FROM tmp), outra (c1) AS (SELECT 1)
        SELECT * FROM base JOIN outra ON 1=1
    """
    found = find_tables(sql)
    assert found.defined == {"tmp", "base", "outra"}
    assert [r.name for r in found.refs] == ["fonte", "tmp", "base", "outra"]


def test_numero_da_linha():
    sql = "SELECT *\nFROM a\n\nJOIN b ON 1=1;\nSELECT 1 FROM c"
    assert [(r.name, r.line) for r in find_tables(sql).refs] == [("a", 2), ("b", 4), ("c", 5)]


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
